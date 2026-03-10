package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import java.util.concurrent.ExecutionException;

/**
 * Сервис для демонстрации replay: перечитывание сообщений из Kafka с любого offset.
 * Использует ручной KafkaConsumer (не @KafkaListener), чтобы управлять offset-ами.
 */
@Service
@Slf4j
public class ReplayService {

    private static final String TOPIC = "replay-topic";
    private static final String GROUP_ID = "replay-demo-group";
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AdminClient adminClient;
    private final String bootstrapServers;

    // Кеш для getStatus() — чтобы не создавать consumer каждые 3 секунды
    private volatile Map<String, Object> cachedStatus = new HashMap<>();
    private volatile long cachedStatusTime = 0;
    private volatile boolean statusDirty = true;

    public ReplayService(KafkaTemplate<String, String> kafkaTemplate,
                         AdminClient adminClient,
                         @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.kafkaTemplate = kafkaTemplate;
        this.adminClient = adminClient;
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Отправляет N тестовых сообщений в replay-topic.
     */
    public List<Map<String, String>> sendMessages(int count) {
        List<Map<String, String>> sent = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            String key = "order-" + i;
            String value = "Заказ #" + i + " — " + LocalDateTime.now().format(TIME_FMT);
            kafkaTemplate.send(TOPIC, key, value);
            sent.add(Map.of("key", key, "value", value));
        }
        kafkaTemplate.flush();
        markDirty();
        log.info("Replay: sent {} messages to {}", count, TOPIC);
        return sent;
    }

    /**
     * Отправляет одно пользовательское сообщение.
     */
    public Map<String, String> sendCustomMessage(String message) {
        String key = "custom-" + System.currentTimeMillis();
        kafkaTemplate.send(TOPIC, key, message);
        kafkaTemplate.flush();
        markDirty();
        log.info("Replay: sent custom message to {}: {}", TOPIC, message);
        return Map.of("key", key, "value", message);
    }

    /**
     * Читает сообщения из replay-topic consumer group-ой replay-demo-group.
     * Читает только непрочитанные (от текущего committed offset).
     */
    public Map<String, Object> readMessages() {
        List<Map<String, String>> messages = new ArrayList<>();
        long startOffset = -1;
        long endOffset = -1;

        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            List<TopicPartition> partitions = getPartitions(consumer);
            consumer.assign(partitions);

            // Восстанавливаем committed offsets
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                if (committed != null) {
                    consumer.seek(tp, committed.offset());
                } else {
                    consumer.seekToBeginning(List.of(tp));
                }
            }

            if (!partitions.isEmpty()) {
                TopicPartition tp0 = partitions.get(0);
                startOffset = consumer.position(tp0);
            }

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

            // Коммитим offsets
            consumer.commitSync();
            markDirty();
            log.info("Replay: read {} messages (offsets {} → {})", messages.size(), startOffset, endOffset);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("messages", messages);
        result.put("count", messages.size());
        result.put("startOffset", startOffset);
        result.put("endOffset", endOffset);
        return result;
    }

    /**
     * Сбрасывает offset consumer group на 0 (начало topic).
     */
    public Map<String, Object> resetToBeginning() throws ExecutionException, InterruptedException {
        Map<String, Object> result = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            List<TopicPartition> partitions = getPartitions(consumer);

            // Получаем текущий committed offset
            Map<TopicPartition, Long> beforeOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                beforeOffsets.put(tp, committed != null ? committed.offset() : 0);
            }

            // Сбрасываем на начало
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                resetOffsets.put(tp, new OffsetAndMetadata(0));
            }
            adminClient.alterConsumerGroupOffsets(GROUP_ID, resetOffsets).all().get();
            markDirty();

            result.put("success", true);
            result.put("partitions", partitions.size());
            result.put("beforeOffsets", beforeOffsets.entrySet().stream()
                    .map(e -> Map.of("partition", e.getKey().partition(), "offset", e.getValue()))
                    .toList());

            log.info("Replay: reset offsets to 0 for group {} on topic {}", GROUP_ID, TOPIC);
        }
        return result;
    }

    /**
     * Сбрасывает offset на конкретное значение.
     */
    public void resetToOffset(long offset) throws ExecutionException, InterruptedException {
        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            List<TopicPartition> partitions = getPartitions(consumer);
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                resetOffsets.put(tp, new OffsetAndMetadata(offset));
            }
            adminClient.alterConsumerGroupOffsets(GROUP_ID, resetOffsets).all().get();
            markDirty();
            log.info("Replay: reset offsets to {} for group {}", offset, GROUP_ID);
        }
    }

    /**
     * Возвращает текущее состояние: committed offsets, end offsets, все сообщения в topic.
     * Использует кеш — consumer создаётся только если данные устарели или помечены как dirty.
     */
    public Map<String, Object> getStatus() {
        long now = System.currentTimeMillis();
        if (!statusDirty && (now - cachedStatusTime) < 5000) {
            return cachedStatus;
        }

        Map<String, Object> status = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            List<TopicPartition> partitions = getPartitions(consumer);
            consumer.assign(partitions);

            // End offsets
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            long totalMessages = endOffsets.values().stream().mapToLong(Long::longValue).sum();

            // Committed offsets
            long committedTotal = 0;
            List<Map<String, Object>> partitionInfo = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
                long committedOffset = committed != null ? committed.offset() : 0;
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                committedTotal += committedOffset;
                partitionInfo.add(Map.of(
                        "partition", tp.partition(),
                        "committed", committedOffset,
                        "end", endOffset,
                        "lag", endOffset - committedOffset
                ));
            }

            // Читаем ВСЕ сообщения в topic (seekToBeginning, без коммита)
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
            status.put("partitions", partitionInfo);
            status.put("allMessages", allMessages);
            status.put("unread", totalMessages - committedTotal);

            cachedStatus = status;
            cachedStatusTime = now;
            statusDirty = false;

        } catch (Exception e) {
            log.error("Replay: error getting status", e);
            status.put("error", e.getMessage());
        }

        return status;
    }

    private void markDirty() {
        statusDirty = true;
    }

    /**
     * Удаляет все сообщения из topic (удаляет и пересоздаёт topic).
     */
    public void clearTopic() throws ExecutionException, InterruptedException {
        // Удаляем consumer group
        try {
            adminClient.deleteConsumerGroups(List.of(GROUP_ID)).all().get();
        } catch (Exception ignored) {}

        // Удаляем topic
        adminClient.deleteTopics(List.of(TOPIC)).all().get();
        log.info("Replay: deleted topic {}", TOPIC);

        // Ждём пока Kafka удалит topic
        Thread.sleep(1000);

        // Пересоздаём topic
        adminClient.createTopics(List.of(
                new org.apache.kafka.clients.admin.NewTopic(TOPIC, 1, (short) 1)
        )).all().get();
        markDirty();
        log.info("Replay: recreated topic {}", TOPIC);
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(props);
    }

    private List<TopicPartition> getPartitions(KafkaConsumer<String, String> consumer) {
        return consumer.partitionsFor(TOPIC).stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }
}
