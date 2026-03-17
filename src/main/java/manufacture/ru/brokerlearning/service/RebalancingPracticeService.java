package manufacture.ru.brokerlearning.service;

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
public class RebalancingPracticeService {

    private static final int MAX_PARTITIONS = 20;
    private static final int MAX_CONSUMERS = 10;
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private static class PracticeSession {
        volatile String currentTopic;
        volatile String currentGroupId;
        volatile int currentPartitions;
        volatile boolean sessionActive = false;
        final AtomicInteger consumerCounter = new AtomicInteger(0);
        final Map<String, PracticeConsumerHandle> handles = new ConcurrentHashMap<>();
        final Map<String, Set<Integer>> assignments = new ConcurrentHashMap<>();
        final List<Map<String, String>> eventLog = Collections.synchronizedList(new ArrayList<>());
    }

    private static class PracticeConsumerHandle {
        volatile Future<?> future;
        volatile KafkaConsumer<String, String> consumer;
        volatile boolean running = true;
    }

    private final ConcurrentHashMap<String, PracticeSession> sessions = new ConcurrentHashMap<>();

    public RebalancingPracticeService(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
    }

    private PracticeSession session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new PracticeSession());
    }

    public synchronized Map<String, Object> createSession(String sid, String topicName, String groupId, int partitions) {
        PracticeSession s = session(sid);
        if (s.sessionActive) doReset(s);

        topicName = topicName.trim();
        groupId = groupId.trim();
        if (topicName.isEmpty() || groupId.isEmpty()) return Map.of("error", "Имя топика и группы не могут быть пустыми");
        if (partitions < 1 || partitions > MAX_PARTITIONS) return Map.of("error", "Количество партиций: от 1 до " + MAX_PARTITIONS);

        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            if (existing.contains(topicName)) {
                var desc = admin.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName);
                int ep = desc.partitions().size();
                if (partitions > ep) {
                    admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(partitions))).all().get();
                    s.currentPartitions = partitions;
                } else {
                    s.currentPartitions = ep;
                }
            } else {
                admin.createTopics(List.of(new NewTopic(topicName, partitions, (short) 1))).all().get();
                s.currentPartitions = partitions;
            }
        } catch (Exception e) {
            return Map.of("error", "Ошибка: " + e.getMessage());
        }

        s.currentTopic = topicName;
        s.currentGroupId = groupId;
        s.sessionActive = true;
        s.consumerCounter.set(0);
        addEvent(s, "SESSION", "—", "Топик: " + topicName + ", группа: " + groupId + ", партиций: " + s.currentPartitions);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> addConsumer(String sid) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Сначала создайте сессию");
        if (s.handles.size() >= MAX_CONSUMERS) return getStatus(sid);

        String id = "Consumer-" + s.consumerCounter.incrementAndGet();
        PracticeConsumerHandle handle = new PracticeConsumerHandle();
        s.handles.put(id, handle);
        handle.future = executor.submit(() -> runConsumer(s, id, handle));
        addEvent(s, "ADD", id, "Добавлен, ожидание rebalance...");
        waitForStable(s);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> removeConsumer(String sid) {
        PracticeSession s = session(sid);
        if (!s.sessionActive || s.handles.isEmpty()) return getStatus(sid);
        String lastId = s.handles.keySet().stream()
                .max(Comparator.comparingInt(k -> Integer.parseInt(k.split("-")[1]))).orElse(null);
        if (lastId == null) return getStatus(sid);

        PracticeConsumerHandle handle = s.handles.remove(lastId);
        handle.running = false;
        if (handle.consumer != null) handle.consumer.wakeup();
        if (handle.future != null) { try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {} }
        s.assignments.remove(lastId);
        addEvent(s, "REMOVE", lastId, "Удалён, rebalance...");
        if (!s.handles.isEmpty()) waitForStable(s);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> addPartition(String sid) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Сначала создайте сессию");
        if (s.currentPartitions >= MAX_PARTITIONS) return getStatus(sid);

        int nc = s.currentPartitions + 1;
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createPartitions(Map.of(s.currentTopic, NewPartitions.increaseTo(nc))).all().get();
            s.currentPartitions = nc;
            addEvent(s, "PARTITION", "—", "P" + (nc - 1) + " добавлена (всего: " + nc + ")");
        } catch (Exception e) {
            addEvent(s, "PARTITION", "—", "Ошибка: " + e.getMessage());
            return getStatus(sid);
        }
        if (!s.handles.isEmpty()) waitForStable(s);
        return getStatus(sid);
    }

    public synchronized Map<String, Object> reset(String sid) {
        PracticeSession s = session(sid);
        doReset(s);
        return getStatus(sid);
    }

    public Map<String, Object> getStatus(String sid) {
        PracticeSession s = session(sid);
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("sessionActive", s.sessionActive);
        status.put("topicName", s.currentTopic);
        status.put("groupId", s.currentGroupId);
        status.put("totalPartitions", s.currentPartitions);
        status.put("maxPartitions", MAX_PARTITIONS);
        status.put("consumerCount", s.handles.size());
        status.put("maxConsumers", MAX_CONSUMERS);
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
        PracticeSession s = sessions.remove(sid);
        if (s != null) doReset(s);
    }

    private void doReset(PracticeSession s) {
        for (var h : s.handles.values()) {
            h.running = false;
            if (h.consumer != null) h.consumer.wakeup();
        }
        for (var h : s.handles.values()) {
            if (h.future != null) { try { h.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {} }
        }
        s.handles.clear();
        s.assignments.clear();
        s.eventLog.clear();
        s.consumerCounter.set(0);
        s.sessionActive = false;
    }

    private void runConsumer(PracticeSession s, String consumerId, PracticeConsumerHandle handle) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, s.currentGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "60000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            handle.consumer = consumer;
            consumer.subscribe(List.of(s.currentTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
                    s.assignments.remove(consumerId);
                    if (!partitions.isEmpty()) {
                        Set<Integer> revoked = new LinkedHashSet<>();
                        partitions.forEach(tp -> revoked.add(tp.partition()));
                        addEvent(s, "REVOKE", consumerId, "Отобраны: " + revoked);
                    }
                }
                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
                    Set<Integer> assigned = new LinkedHashSet<>();
                    partitions.forEach(tp -> assigned.add(tp.partition()));
                    s.assignments.put(consumerId, assigned);
                    addEvent(s, "ASSIGN", consumerId, assigned.isEmpty() ? "Нет партиций (idle)" : "Назначены: " + assigned);
                }
            });
            while (handle.running) {
                try { consumer.poll(Duration.ofMillis(1000)); }
                catch (WakeupException e) { if (!handle.running) break; }
            }
        } catch (Exception e) {
            log.warn("Practice: {} error: {}", consumerId, e.getMessage());
        }
    }

    private void waitForStable(PracticeSession s) {
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

    private void addEvent(PracticeSession s, String type, String consumer, String message) {
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
}
