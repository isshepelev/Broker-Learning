package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class ReplayService {

    private static final String TOPIC_PREFIX = "replay-topic-";
    private static final String GROUP_PREFIX = "replay-group-";
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AdminClient adminClient;
    private final String bootstrapServers;

    private static class SessionCache {
        volatile Map<String, Object> status = new HashMap<>();
        volatile long time = 0;
        volatile boolean dirty = true;
    }

    private final ConcurrentHashMap<String, SessionCache> caches = new ConcurrentHashMap<>();

    public ReplayService(KafkaTemplate<String, String> kafkaTemplate,
                         AdminClient adminClient,
                         @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.kafkaTemplate = kafkaTemplate;
        this.adminClient = adminClient;
        this.bootstrapServers = bootstrapServers;
    }

    private String topicFor(String sid) { return TOPIC_PREFIX + sid; }
    private String groupFor(String sid) { return GROUP_PREFIX + sid; }

    private SessionCache cache(String sid) {
        return caches.computeIfAbsent(sid, k -> new SessionCache());
    }

    private void ensureTopic(String sid) {
        String topic = topicFor(sid);
        try {
            Set<String> existing = adminClient.listTopics().names().get();
            if (!existing.contains(topic)) {
                adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                log.info("Replay: created session topic {}", topic);
            }
        } catch (Exception e) {
            log.warn("Replay: failed to ensure topic {}: {}", topic, e.getMessage());
        }
    }

    public List<Map<String, String>> sendMessages(String sid, int count) {
        ensureTopic(sid);
        String topic = topicFor(sid);
        List<Map<String, String>> sent = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            String key = "order-" + i;
            String value = "Заказ #" + i + " — " + LocalDateTime.now().format(TIME_FMT);
            kafkaTemplate.send(topic, key, value);
            sent.add(Map.of("key", key, "value", value));
        }
        kafkaTemplate.flush();
        cache(sid).dirty = true;
        return sent;
    }

    public Map<String, String> sendCustomMessage(String sid, String message) {
        ensureTopic(sid);
        String topic = topicFor(sid);
        String key = "custom-" + System.currentTimeMillis();
        kafkaTemplate.send(topic, key, message);
        kafkaTemplate.flush();
        cache(sid).dirty = true;
        return Map.of("key", key, "value", message);
    }

    public Map<String, Object> readMessages(String sid) {
        ensureTopic(sid);
        List<Map<String, String>> messages = new ArrayList<>();
        long startOffset = -1, endOffset = -1;

        try (KafkaConsumer<String, String> consumer = createConsumer(sid)) {
            List<TopicPartition> partitions = getPartitions(consumer, sid);
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
            }
            consumer.commitSync();
            cache(sid).dirty = true;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("messages", messages);
        result.put("count", messages.size());
        result.put("startOffset", startOffset);
        result.put("endOffset", endOffset);
        return result;
    }

    public Map<String, Object> resetToBeginning(String sid) throws ExecutionException, InterruptedException {
        ensureTopic(sid);
        try (KafkaConsumer<String, String> consumer = createConsumer(sid)) {
            List<TopicPartition> partitions = getPartitions(consumer, sid);
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                resetOffsets.put(tp, new OffsetAndMetadata(0));
            }
            adminClient.alterConsumerGroupOffsets(groupFor(sid), resetOffsets).all().get();
            cache(sid).dirty = true;
        }
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        return result;
    }

    public void resetToOffset(String sid, long offset) throws ExecutionException, InterruptedException {
        ensureTopic(sid);
        try (KafkaConsumer<String, String> consumer = createConsumer(sid)) {
            List<TopicPartition> partitions = getPartitions(consumer, sid);
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                resetOffsets.put(tp, new OffsetAndMetadata(offset));
            }
            adminClient.alterConsumerGroupOffsets(groupFor(sid), resetOffsets).all().get();
            cache(sid).dirty = true;
        }
    }

    public Map<String, Object> getStatus(String sid) {
        SessionCache c = cache(sid);
        long now = System.currentTimeMillis();
        if (!c.dirty && (now - c.time) < 5000) return c.status;

        Map<String, Object> status = new HashMap<>();
        String topic = topicFor(sid);

        try {
            Set<String> existing = adminClient.listTopics().names().get();
            if (!existing.contains(topic)) {
                status.put("totalMessages", 0);
                status.put("committedOffset", 0);
                status.put("allMessages", List.of());
                status.put("unread", 0);
                c.status = status; c.time = now; c.dirty = false;
                return status;
            }
        } catch (Exception e) {
            status.put("error", e.getMessage());
            return status;
        }

        try (KafkaConsumer<String, String> consumer = createConsumer(sid)) {
            List<TopicPartition> partitions = getPartitions(consumer, sid);
            consumer.assign(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            long totalMessages = endOffsets.values().stream().mapToLong(Long::longValue).sum();

            long committedTotal = 0;
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                committedTotal += committed != null ? committed.offset() : 0;
            }

            consumer.seekToBeginning(partitions);
            List<Map<String, String>> allMessages = new ArrayList<>();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                allMessages.add(Map.of(
                        "key", record.key() != null ? record.key() : "-",
                        "value", record.value() != null ? record.value() : "",
                        "partition", String.valueOf(record.partition()),
                        "offset", String.valueOf(record.offset())
                ));
            }

            status.put("totalMessages", totalMessages);
            status.put("committedOffset", committedTotal);
            status.put("allMessages", allMessages);
            status.put("unread", totalMessages - committedTotal);

            c.status = status; c.time = now; c.dirty = false;
        } catch (Exception e) {
            log.error("Replay: error getting status for session {}", sid, e);
            status.put("error", e.getMessage());
        }
        return status;
    }

    public void clearTopic(String sid) throws ExecutionException, InterruptedException {
        String topic = topicFor(sid);
        String group = groupFor(sid);
        try { adminClient.deleteConsumerGroups(List.of(group)).all().get(); } catch (Exception ignored) {}
        try {
            adminClient.deleteTopics(List.of(topic)).all().get();
            Thread.sleep(1000);
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        } catch (Exception e) {
            log.warn("Replay: clear topic {} error: {}", topic, e.getMessage());
        }
        cache(sid).dirty = true;
    }

    /** Called on session destroy to clean up Kafka resources */
    public void cleanupSession(String sid) {
        caches.remove(sid);
        String topic = topicFor(sid);
        String group = groupFor(sid);
        try { adminClient.deleteConsumerGroups(List.of(group)).all().get(); } catch (Exception ignored) {}
        try { adminClient.deleteTopics(List.of(topic)).all().get(); } catch (Exception ignored) {}
        log.info("Replay: cleaned up session {} (topic={}, group={})", sid, topic, group);
    }

    private KafkaConsumer<String, String> createConsumer(String sid) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupFor(sid));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(props);
    }

    private List<TopicPartition> getPartitions(KafkaConsumer<String, String> consumer, String sid) {
        return consumer.partitionsFor(topicFor(sid)).stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }
}
