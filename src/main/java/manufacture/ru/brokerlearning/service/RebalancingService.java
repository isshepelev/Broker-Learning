package manufacture.ru.brokerlearning.service;
import manufacture.ru.brokerlearning.config.UserSessionHelper;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class RebalancingService {

    private static final String TOPIC_PREFIX = "rebalancing-topic-";
    private static final String GROUP_PREFIX = "rebalancing-group-";
    private static final int INITIAL_PARTITIONS = 6;
    private static final int MAX_PARTITIONS = 12;
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private static class SessionState {
        final AtomicInteger counter = new AtomicInteger(0);
        volatile int currentPartitions = INITIAL_PARTITIONS;
        final Map<String, ConsumerHandle> handles = new ConcurrentHashMap<>();
        final Map<String, Set<Integer>> assignments = new ConcurrentHashMap<>();
        final List<Map<String, String>> eventLog = Collections.synchronizedList(new ArrayList<>());
    }

    private final ConcurrentHashMap<String, SessionState> sessions = new ConcurrentHashMap<>();

    private static class ConsumerHandle {
        volatile Future<?> future;
        volatile KafkaConsumer<String, String> consumer;
        volatile boolean running = true;
    }

    public RebalancingService(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
    }

    private SessionState session(String sid) {
        return sessions.computeIfAbsent(sid, k -> {
            SessionState s = new SessionState();
            ensureTopic(sid, s);
            return s;
        });
    }

    private String topicFor(String sid) { return UserSessionHelper.isAdminSid(sid) ? "rebalancing-topic" : TOPIC_PREFIX + sid; }
    private String groupFor(String sid) { return UserSessionHelper.isAdminSid(sid) ? "rebalancing-group" : GROUP_PREFIX + sid; }

    public synchronized Map<String, Object> addConsumer(String sid) {
        SessionState s = session(sid);
        String id = "Consumer-" + s.counter.incrementAndGet();

        ConsumerHandle handle = new ConsumerHandle();
        s.handles.put(id, handle);
        handle.future = executor.submit(() -> runConsumer(sid, id, handle));

        addEvent(s, "ADD", id, "Добавлен в группу, ожидание rebalance...");
        waitForStableAssignments(s);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> removeConsumer(String sid) {
        SessionState s = session(sid);
        if (s.handles.isEmpty()) return getStatus(sid);

        String lastId = s.handles.keySet().stream()
                .max(Comparator.comparingInt(k -> Integer.parseInt(k.split("-")[1])))
                .orElse(null);
        if (lastId == null) return getStatus(sid);

        ConsumerHandle handle = s.handles.remove(lastId);
        handle.running = false;
        if (handle.consumer != null) handle.consumer.wakeup();
        if (handle.future != null) {
            try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
        s.assignments.remove(lastId);
        addEvent(s, "REMOVE", lastId, "Удалён из группы, запущен rebalance...");

        if (!s.handles.isEmpty()) waitForStableAssignments(s);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> addPartition(String sid) {
        SessionState s = session(sid);
        if (s.currentPartitions >= MAX_PARTITIONS) {
            addEvent(s, "PARTITION", "—", "Достигнут лимит: " + MAX_PARTITIONS + " партиций");
            return getStatus(sid);
        }
        int newCount = s.currentPartitions + 1;
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createPartitions(Map.of(topicFor(sid), NewPartitions.increaseTo(newCount))).all().get();
            s.currentPartitions = newCount;
            addEvent(s, "PARTITION", "—", "Добавлена партиция P" + (newCount - 1) + " (всего: " + newCount + ")");
        } catch (Exception e) {
            addEvent(s, "PARTITION", "—", "Ошибка: " + e.getMessage());
            return getStatus(sid);
        }
        if (!s.handles.isEmpty()) waitForStableAssignments(s);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> reset(String sid) {
        SessionState s = session(sid);
        for (var handle : s.handles.values()) {
            handle.running = false;
            if (handle.consumer != null) handle.consumer.wakeup();
        }
        for (var handle : s.handles.values()) {
            if (handle.future != null) {
                try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
            }
        }
        s.handles.clear();
        s.assignments.clear();
        s.eventLog.clear();
        s.counter.set(0);
        resetTopic(sid);
        s.currentPartitions = INITIAL_PARTITIONS;
        addEvent(s, "RESET", "—", "Все consumer'ы остановлены, партиции сброшены до " + INITIAL_PARTITIONS);
        return getStatus(sid);
    }

    public Map<String, Object> getStatus(String sid) {
        SessionState s = session(sid);
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("totalPartitions", s.currentPartitions);
        status.put("maxPartitions", MAX_PARTITIONS);
        status.put("consumerCount", s.handles.size());
        status.put("assignments", new LinkedHashMap<>(s.assignments));
        status.put("events", new ArrayList<>(s.eventLog));
        status.put("allConsumers", new ArrayList<>(s.handles.keySet()));

        Set<Integer> allAssigned = new HashSet<>();
        s.assignments.values().forEach(allAssigned::addAll);
        Set<Integer> unassigned = new LinkedHashSet<>();
        for (int i = 0; i < s.currentPartitions; i++) {
            if (!allAssigned.contains(i)) unassigned.add(i);
        }
        status.put("unassigned", unassigned);
        return status;
    }

    public void cleanupSession(String sid) {
        SessionState s = sessions.remove(sid);
        if (s == null) return;
        for (var handle : s.handles.values()) {
            handle.running = false;
            if (handle.consumer != null) handle.consumer.wakeup();
        }
        try (AdminClient admin = AdminClient.create(adminProps())) {
            try { admin.deleteConsumerGroups(List.of(groupFor(sid))).all().get(); } catch (Exception ignored) {}
            try { admin.deleteTopics(List.of(topicFor(sid))).all().get(); } catch (Exception ignored) {}
        } catch (Exception ignored) {}
    }

    private void waitForStableAssignments(SessionState s) {
        long deadline = System.currentTimeMillis() + 15_000;
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        while (System.currentTimeMillis() < deadline) {
            Set<Integer> all = new HashSet<>();
            for (String cId : s.handles.keySet()) {
                Set<Integer> parts = s.assignments.get(cId);
                if (parts != null) all.addAll(parts);
            }
            if (all.size() == s.currentPartitions) return;
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        }
    }

    private void runConsumer(String sid, String consumerId, ConsumerHandle handle) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupFor(sid));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "60000");

        SessionState s = sessions.get(sid);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            handle.consumer = consumer;
            consumer.subscribe(List.of(topicFor(sid)), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
                    if (s != null) {
                        s.assignments.remove(consumerId);
                        if (!partitions.isEmpty()) {
                            Set<Integer> revoked = new LinkedHashSet<>();
                            partitions.forEach(tp -> revoked.add(tp.partition()));
                            addEvent(s, "REVOKE", consumerId, "Отобраны партиции: " + revoked);
                        }
                    }
                }
                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
                    if (s != null) {
                        Set<Integer> assigned = new LinkedHashSet<>();
                        partitions.forEach(tp -> assigned.add(tp.partition()));
                        s.assignments.put(consumerId, assigned);
                        addEvent(s, "ASSIGN", consumerId, assigned.isEmpty() ? "Нет партиций (idle)" : "Назначены партиции: " + assigned);
                    }
                }
            });
            while (handle.running) {
                try { consumer.poll(Duration.ofMillis(1000)); }
                catch (WakeupException e) { if (!handle.running) break; }
            }
        } catch (Exception e) {
            log.warn("Rebalancing: {} error: {}", consumerId, e.getMessage());
        }
    }

    private void addEvent(SessionState s, String type, String consumer, String message) {
        synchronized (s.eventLog) {
            s.eventLog.add(0, Map.of("time", LocalTime.now().format(FMT), "type", type, "consumer", consumer, "message", message));
            while (s.eventLog.size() > 50) s.eventLog.remove(s.eventLog.size() - 1);
        }
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        return props;
    }

    private void ensureTopic(String sid, SessionState s) {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            String topic = topicFor(sid);
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(topic)) {
                admin.createTopics(List.of(new NewTopic(topic, INITIAL_PARTITIONS, (short) 1))).all().get();
            } else {
                var desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                if (desc != null) s.currentPartitions = desc.partitions().size();
            }
        } catch (Exception e) {
            log.warn("ensureTopic error: {}", e.getMessage());
        }
    }

    private void resetTopic(String sid) {
        String topic = topicFor(sid);
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.deleteTopics(List.of(topic)).all().get();
            for (int i = 0; i < 20; i++) {
                Thread.sleep(500);
                if (!admin.listTopics().names().get().contains(topic)) break;
            }
            admin.createTopics(List.of(new NewTopic(topic, INITIAL_PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            log.warn("resetTopic error: {}", e.getMessage());
        }
    }
}
