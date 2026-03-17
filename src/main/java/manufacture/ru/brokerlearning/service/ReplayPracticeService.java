package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.InternalKafkaRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class ReplayPracticeService {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final AdminClient adminClient;
    private final String bootstrapServers;

    private static class PracticeSession {
        volatile String currentTopic;
        volatile boolean sessionActive = false;
        final Set<String> consumerGroups = ConcurrentHashMap.newKeySet();
        final AtomicInteger msgCounter = new AtomicInteger(0);
        volatile Map<String, Object> cachedStatus = new HashMap<>();
        volatile long cachedStatusTime = 0;
        volatile boolean statusDirty = true;
    }

    private final ConcurrentHashMap<String, PracticeSession> sessions = new ConcurrentHashMap<>();

    public ReplayPracticeService(AdminClient adminClient,
                                 @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.adminClient = adminClient;
        this.bootstrapServers = bootstrapServers;
    }

    private PracticeSession session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new PracticeSession());
    }

    public Map<String, Object> createTopic(String topicName) {
        topicName = topicName.trim();
        if (topicName.isEmpty()) return Map.of("error", "Имя топика не может быть пустым");
        if (InternalKafkaRegistry.isInternalTopic(topicName)) return Map.of("error", "Нельзя использовать внутренний топик");
        try {
            Set<String> existing = adminClient.listTopics().names().get();
            if (existing.contains(topicName)) return Map.of("error", "Топик '" + topicName + "' уже существует");
            adminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
            return Map.of("success", true, "topic", topicName);
        } catch (Exception e) {
            return Map.of("error", "Ошибка: " + e.getMessage());
        }
    }

    public Map<String, Object> startSession(String sid, String topicName) {
        topicName = topicName.trim();
        if (topicName.isEmpty()) return Map.of("error", "Имя топика не может быть пустым");
        if (InternalKafkaRegistry.isInternalTopic(topicName)) return Map.of("error", "Нельзя использовать внутренний топик");
        try {
            Set<String> existing = adminClient.listTopics().names().get();
            if (!existing.contains(topicName)) return Map.of("error", "Топик '" + topicName + "' не найден.");
        } catch (Exception e) {
            return Map.of("error", "Ошибка: " + e.getMessage());
        }

        PracticeSession s = session(sid);
        s.currentTopic = topicName;
        s.sessionActive = true;
        s.consumerGroups.clear();
        s.msgCounter.set(0);
        s.statusDirty = true;
        return Map.of("success", true, "topic", topicName);
    }

    public Map<String, Object> addConsumer(String sid, String groupId) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Сначала начните сессию");
        groupId = groupId.trim();
        if (groupId.isEmpty()) return Map.of("error", "Имя группы не может быть пустым");
        if (s.consumerGroups.size() >= 5) return Map.of("error", "Максимум 5 consumer group");
        s.consumerGroups.add(groupId);
        s.statusDirty = true;
        return Map.of("success", true, "groupId", groupId);
    }

    public Map<String, Object> removeConsumer(String sid, String groupId) {
        PracticeSession s = session(sid);
        s.consumerGroups.remove(groupId);
        s.statusDirty = true;
        return Map.of("success", true);
    }

    public Map<String, Object> sendMessages(String sid, int count) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Сначала начните сессию");
        if (count < 1) count = 1;
        if (count > 50) count = 50;

        List<Map<String, String>> sent = new ArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < count; i++) {
                int num = s.msgCounter.incrementAndGet();
                String key = "msg-" + num;
                String value = "Сообщение #" + num + " — " + LocalDateTime.now().format(TIME_FMT);
                producer.send(new ProducerRecord<>(s.currentTopic, key, value));
                sent.add(Map.of("key", key, "value", value));
            }
            producer.flush();
        } catch (Exception e) {
            return Map.of("error", "Ошибка отправки: " + e.getMessage());
        }
        s.statusDirty = true;
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("count", sent.size());
        return result;
    }

    public Map<String, Object> sendCustomMessage(String sid, String message) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Сначала начните сессию");
        String key = "custom-" + System.currentTimeMillis();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            producer.send(new ProducerRecord<>(s.currentTopic, key, message));
            producer.flush();
        } catch (Exception e) {
            return Map.of("error", "Ошибка: " + e.getMessage());
        }
        s.statusDirty = true;
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("count", 1);
        return result;
    }

    public Map<String, Object> readMessages(String sid, String groupId, String mode) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Нет активной сессии");
        if (!s.consumerGroups.contains(groupId)) return Map.of("error", "Consumer group не найден");

        boolean readOne = "one".equals(mode);
        List<Map<String, String>> messages = new ArrayList<>();
        long startOffset = -1, endOffset = -1;

        try (KafkaConsumer<String, String> consumer = createConsumer(groupId)) {
            List<TopicPartition> partitions = getPartitions(consumer, s.currentTopic);
            consumer.assign(partitions);
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                if (committed != null) consumer.seek(tp, committed.offset());
                else consumer.seekToBeginning(List.of(tp));
            }
            if (!partitions.isEmpty()) startOffset = consumer.position(partitions.get(0));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, String> record : records) {
                messages.add(Map.of(
                        "key", record.key() != null ? record.key() : "-",
                        "value", record.value() != null ? record.value() : "",
                        "partition", String.valueOf(record.partition()),
                        "offset", String.valueOf(record.offset()),
                        "time", LocalDateTime.now().format(TIME_FMT)
                ));
                endOffset = record.offset();
                if (readOne) break;
            }
            if (!messages.isEmpty()) {
                if (readOne) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    Map<String, String> msg = messages.get(0);
                    offsets.put(new TopicPartition(s.currentTopic, Integer.parseInt(msg.get("partition"))),
                            new OffsetAndMetadata(Long.parseLong(msg.get("offset")) + 1));
                    consumer.commitSync(offsets);
                } else {
                    consumer.commitSync();
                }
            }
            s.statusDirty = true;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("messages", messages);
        result.put("count", messages.size());
        result.put("startOffset", startOffset);
        result.put("endOffset", endOffset);
        return result;
    }

    public Map<String, Object> resetToBeginning(String sid, String groupId) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Нет активной сессии");
        try (KafkaConsumer<String, String> consumer = createConsumer(groupId)) {
            List<TopicPartition> partitions = getPartitions(consumer, s.currentTopic);
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) resetOffsets.put(tp, new OffsetAndMetadata(0));
            adminClient.alterConsumerGroupOffsets(groupId, resetOffsets).all().get();
            s.statusDirty = true;
        } catch (Exception e) {
            return Map.of("success", false, "error", e.getMessage());
        }
        return Map.of("success", true);
    }

    public Map<String, Object> resetToOffset(String sid, String groupId, long offset) {
        PracticeSession s = session(sid);
        if (!s.sessionActive) return Map.of("error", "Нет активной сессии");
        try (KafkaConsumer<String, String> consumer = createConsumer(groupId)) {
            List<TopicPartition> partitions = getPartitions(consumer, s.currentTopic);
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) resetOffsets.put(tp, new OffsetAndMetadata(offset));
            adminClient.alterConsumerGroupOffsets(groupId, resetOffsets).all().get();
            s.statusDirty = true;
        } catch (Exception e) {
            return Map.of("success", false, "error", e.getMessage());
        }
        return Map.of("success", true);
    }

    public Map<String, Object> getStatus(String sid) {
        PracticeSession s = session(sid);
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("sessionActive", s.sessionActive);
        status.put("topicName", s.currentTopic);
        status.put("consumers", new ArrayList<>(s.consumerGroups));

        if (!s.sessionActive) return status;

        long now = System.currentTimeMillis();
        if (!s.statusDirty && (now - s.cachedStatusTime) < 5000) return s.cachedStatus;

        try {
            List<Map<String, String>> allMessages = new ArrayList<>();
            long totalMessages = 0;

            try (KafkaConsumer<String, String> tmpConsumer = createConsumer("_practice-status-" + sid)) {
                List<TopicPartition> partitions = getPartitions(tmpConsumer, s.currentTopic);
                tmpConsumer.assign(partitions);
                Map<TopicPartition, Long> endOffsets = tmpConsumer.endOffsets(partitions);
                totalMessages = endOffsets.values().stream().mapToLong(Long::longValue).sum();
                tmpConsumer.seekToBeginning(partitions);
                ConsumerRecords<String, String> records = tmpConsumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    allMessages.add(Map.of(
                            "key", record.key() != null ? record.key() : "-",
                            "value", record.value() != null ? record.value() : "",
                            "partition", String.valueOf(record.partition()),
                            "offset", String.valueOf(record.offset())
                    ));
                }
            }
            status.put("totalMessages", totalMessages);
            status.put("allMessages", allMessages);

            List<Map<String, Object>> consumerStatuses = new ArrayList<>();
            for (String groupId : s.consumerGroups) {
                Map<String, Object> cs = new LinkedHashMap<>();
                cs.put("groupId", groupId);
                try (KafkaConsumer<String, String> consumer = createConsumer(groupId)) {
                    List<TopicPartition> partitions = getPartitions(consumer, s.currentTopic);
                    long committedTotal = 0;
                    for (TopicPartition tp : partitions) {
                        OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                        committedTotal += committed != null ? committed.offset() : 0;
                    }
                    cs.put("committedOffset", committedTotal);
                    cs.put("unread", totalMessages - committedTotal);
                } catch (Exception e) {
                    cs.put("committedOffset", 0);
                    cs.put("unread", totalMessages);
                }
                consumerStatuses.add(cs);
            }
            status.put("consumerStatuses", consumerStatuses);

            s.cachedStatus = status;
            s.cachedStatusTime = now;
            s.statusDirty = false;
        } catch (Exception e) {
            log.error("ReplayPractice: status error for session {}", sid, e);
            status.put("error", e.getMessage());
        }
        return status;
    }

    public Set<String> listTopics() {
        try {
            Set<String> all = adminClient.listTopics().names().get();
            Set<String> filtered = new LinkedHashSet<>();
            for (String t : all) {
                if (InternalKafkaRegistry.isUserTopic(t) && !t.startsWith("replay-topic-")) filtered.add(t);
            }
            return filtered;
        } catch (Exception e) {
            return Set.of();
        }
    }

    public List<String> listConsumerGroups() {
        try {
            return adminClient.listConsumerGroups().all().get().stream()
                    .map(g -> g.groupId())
                    .filter(InternalKafkaRegistry::isUserGroup)
                    .filter(g -> !g.startsWith("replay-group-") && !g.startsWith("_practice-status-"))
                    .sorted()
                    .toList();
        } catch (Exception e) {
            return List.of();
        }
    }

    public Map<String, Object> endSession(String sid) {
        sessions.remove(sid);
        return Map.of("success", true);
    }

    public void cleanupSession(String sid) {
        sessions.remove(sid);
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(props);
    }

    private List<TopicPartition> getPartitions(KafkaConsumer<String, String> consumer, String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
