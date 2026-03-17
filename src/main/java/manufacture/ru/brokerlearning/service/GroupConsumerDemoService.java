package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Демонстрация: несколько consumer-ов в ОДНОЙ группе читают один топик.
 * Kafka распределяет партиции между consumer-ами — каждое сообщение читает ровно один consumer.
 */
@Service
@Slf4j
public class GroupConsumerDemoService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final int PARTITIONS = 6;

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private static class DemoSession {
        final String topic;
        final String groupId;
        final AtomicInteger msgCounter = new AtomicInteger(0);
        final AtomicInteger consumerCounter = new AtomicInteger(0);
        final Map<String, ConsumerHandle> consumers = new ConcurrentHashMap<>();
        /** consumerId → list of messages this consumer read */
        final Map<String, List<Map<String, Object>>> readMessages = new ConcurrentHashMap<>();
        final List<Map<String, String>> eventLog = Collections.synchronizedList(new ArrayList<>());

        boolean autoCreated = false;
        int actualPartitions = PARTITIONS;

        DemoSession(String topic, String groupId) {
            this.topic = topic;
            this.groupId = groupId;
        }
    }

    private static class ConsumerHandle {
        volatile KafkaConsumer<String, String> consumer;
        volatile Future<?> future;
        volatile boolean running = true;
        final Set<Integer> assignedPartitions = ConcurrentHashMap.newKeySet();
    }

    private final ConcurrentHashMap<String, DemoSession> sessions = new ConcurrentHashMap<>();

    public GroupConsumerDemoService(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
    }

    /**
     * @param customTopic null or empty = auto-create demo topic; otherwise use existing topic
     */
    public Map<String, Object> startSession(String sid, String customTopic) {
        DemoSession old = sessions.remove(sid);
        if (old != null) doCleanup(old);

        String topic;
        boolean autoCreated = false;
        if (customTopic != null && !customTopic.isBlank()) {
            topic = customTopic.trim();
        } else {
            topic = UserSessionHelper.isAdminSid(sid) ? "group-demo-topic" : "group-demo-topic-" + sid;
            ensureTopic(topic);
            autoCreated = true;
        }
        String groupId = UserSessionHelper.isAdminSid(sid) ? "group-demo-group" : "group-demo-group-" + sid;

        DemoSession s = new DemoSession(topic, groupId);
        s.autoCreated = autoCreated;
        // Determine actual partition count
        try (AdminClient admin = AdminClient.create(adminProps())) {
            var desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            if (desc != null) s.actualPartitions = desc.partitions().size();
        } catch (Exception e) {
            s.actualPartitions = PARTITIONS;
        }

        sessions.put(sid, s);
        addEvent(s, "SESSION", "topic=" + topic + " (" + s.actualPartitions + " партиций), group=" + groupId);
        return getStatus(sid);
    }

    public Map<String, Object> addConsumer(String sid) {
        DemoSession s = sessions.get(sid);
        if (s == null) return Map.of("error", "Сначала начните сессию");
        if (s.consumers.size() >= 6) return Map.of("error", "Максимум 6 consumer-ов");

        String id = "C-" + s.consumerCounter.incrementAndGet();
        ConsumerHandle handle = new ConsumerHandle();
        s.consumers.put(id, handle);
        s.readMessages.put(id, Collections.synchronizedList(new ArrayList<>()));
        handle.future = executor.submit(() -> runConsumer(s, id, handle));

        addEvent(s, "ADD", id + " добавлен в группу " + s.groupId);

        // Wait for rebalance
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        return getStatus(sid);
    }

    public Map<String, Object> removeConsumer(String sid) {
        DemoSession s = sessions.get(sid);
        if (s == null || s.consumers.isEmpty()) return getStatus(sid);

        String lastId = s.consumers.keySet().stream()
                .max(Comparator.comparingInt(k -> Integer.parseInt(k.split("-")[1]))).orElse(null);
        if (lastId == null) return getStatus(sid);

        ConsumerHandle handle = s.consumers.remove(lastId);
        handle.running = false;
        if (handle.consumer != null) handle.consumer.wakeup();
        if (handle.future != null) { try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {} }
        s.readMessages.remove(lastId);

        addEvent(s, "REMOVE", lastId + " удалён");
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}

        return getStatus(sid);
    }

    public Map<String, Object> sendMessages(String sid, int count) {
        DemoSession s = sessions.get(sid);
        if (s == null) return Map.of("error", "Сначала начните сессию");
        if (count < 1) count = 1;
        if (count > 50) count = 50;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                int num = s.msgCounter.incrementAndGet();
                String key = "key-" + num;
                String value = "Сообщение #" + num;
                producer.send(new ProducerRecord<>(s.topic, key, value));
            }
            producer.flush();
        }

        addEvent(s, "SEND", "Отправлено " + count + " сообщений (всего: " + s.msgCounter.get() + ")");
        return Map.of("success", true, "count", count);
    }

    public Map<String, Object> sendCustomMessage(String sid, String message) {
        DemoSession s = sessions.get(sid);
        if (s == null) return Map.of("error", "Сначала начните сессию");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        int num = s.msgCounter.incrementAndGet();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(s.topic, "key-" + num, message));
            producer.flush();
        }

        addEvent(s, "SEND", "Отправлено: " + message);
        return Map.of("success", true, "count", 1);
    }

    public Map<String, Object> getStatus(String sid) {
        DemoSession s = sessions.get(sid);
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("active", s != null);
        if (s == null) return status;

        status.put("topic", s.topic);
        status.put("groupId", s.groupId);
        status.put("partitions", s.actualPartitions);
        status.put("totalSent", s.msgCounter.get());

        // Consumer info
        List<Map<String, Object>> consumerInfos = new ArrayList<>();
        for (var entry : s.consumers.entrySet()) {
            String cId = entry.getKey();
            ConsumerHandle h = entry.getValue();
            List<Map<String, Object>> msgs = s.readMessages.getOrDefault(cId, List.of());
            Map<String, Object> ci = new LinkedHashMap<>();
            ci.put("id", cId);
            ci.put("partitions", new TreeSet<>(h.assignedPartitions));
            ci.put("messageCount", msgs.size());
            ci.put("messages", msgs.size() > 50 ? msgs.subList(0, 50) : new ArrayList<>(msgs));
            consumerInfos.add(ci);
        }
        status.put("consumers", consumerInfos);

        // Partition map: which consumer owns which partition
        Map<Integer, String> partitionOwner = new LinkedHashMap<>();
        for (var entry : s.consumers.entrySet()) {
            for (int p : entry.getValue().assignedPartitions) {
                partitionOwner.put(p, entry.getKey());
            }
        }
        // Messages grouped by partition
        Map<Integer, Integer> partitionMsgCount = new LinkedHashMap<>();
        for (var entry : s.readMessages.entrySet()) {
            for (var msg : entry.getValue()) {
                int p = (int) msg.get("partition");
                partitionMsgCount.merge(p, 1, Integer::sum);
            }
        }
        status.put("partitionOwner", partitionOwner);
        status.put("partitionMsgCount", partitionMsgCount);

        status.put("events", new ArrayList<>(s.eventLog));
        return status;
    }

    public Map<String, Object> endSession(String sid) {
        DemoSession s = sessions.remove(sid);
        if (s != null) doCleanup(s);
        return Map.of("success", true);
    }

    public void cleanupSession(String sid) {
        DemoSession s = sessions.remove(sid);
        if (s != null) doCleanup(s);
    }

    private void doCleanup(DemoSession s) {
        for (var h : s.consumers.values()) {
            h.running = false;
            if (h.consumer != null) h.consumer.wakeup();
        }
        for (var h : s.consumers.values()) {
            if (h.future != null) { try { h.future.get(5, TimeUnit.SECONDS); } catch (Exception ignored) {} }
        }
        s.consumers.clear();
        if (s.autoCreated) {
            try (AdminClient admin = AdminClient.create(adminProps())) {
                admin.deleteTopics(List.of(s.topic)).all().get();
            } catch (Exception ignored) {}
        }
    }

    private void runConsumer(DemoSession s, String consumerId, ConsumerHandle handle) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, s.groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            handle.consumer = consumer;
            consumer.subscribe(List.of(s.topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
                    handle.assignedPartitions.clear();
                }
                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
                    handle.assignedPartitions.clear();
                    partitions.forEach(tp -> handle.assignedPartitions.add(tp.partition()));
                    addEvent(s, "ASSIGN", consumerId + " получил партиции: " + handle.assignedPartitions);
                }
            });

            List<Map<String, Object>> myMessages = s.readMessages.get(consumerId);
            while (handle.running) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        if (myMessages != null) {
                            Map<String, Object> msg = new LinkedHashMap<>();
                            msg.put("key", record.key() != null ? record.key() : "");
                            msg.put("value", record.value() != null ? record.value() : "");
                            msg.put("partition", record.partition());
                            msg.put("offset", record.offset());
                            msg.put("time", LocalDateTime.now().format(FMT));
                            msg.put("consumer", consumerId);
                            myMessages.add(0, msg);
                            while (myMessages.size() > 200) myMessages.remove(myMessages.size() - 1);
                        }
                    }
                } catch (WakeupException e) {
                    if (!handle.running) break;
                }
            }
        } catch (Exception e) {
            log.warn("GroupDemo consumer {} error: {}", consumerId, e.getMessage());
        }
    }

    private void addEvent(DemoSession s, String type, String message) {
        synchronized (s.eventLog) {
            s.eventLog.add(0, Map.of("time", LocalDateTime.now().format(FMT), "type", type, "message", message));
            while (s.eventLog.size() > 30) s.eventLog.remove(s.eventLog.size() - 1);
        }
    }

    private void ensureTopic(String topic) {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            if (existing.contains(topic)) {
                admin.deleteTopics(List.of(topic)).all().get();
                Thread.sleep(1000);
            }
            admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            log.warn("ensureTopic error: {}", e.getMessage());
        }
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        return props;
    }
}
