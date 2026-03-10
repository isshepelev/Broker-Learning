package manufacture.ru.brokerlearning.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderingService {

    private static final String TOPIC_1P = "ordering-1p-topic";
    private static final String TOPIC_5P = "ordering-5p-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String bootstrapServers;

    @Getter
    private volatile Map<String, Object> lastResult;

    public OrderingService(KafkaTemplate<String, String> kafkaTemplate,
                           org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs : String.join(",", (java.util.Collection<String>) bs);
    }

    /**
     * Демо 1: БЕЗ ключа (null key) — round-robin распределение.
     * 1 партиция → порядок сохранён.
     * 5 партиций → глобальный порядок НАРУШЕН (но внутри каждой партиции — сохранён).
     */
    public Map<String, Object> runDemo(int count) {
        resetTopics();

        List<Map<String, Object>> sentMessages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            String value = "Сообщение #" + i;
            kafkaTemplate.send(TOPIC_1P, null, value);
            // Явный round-robin по партициям (sticky partitioner иначе кладёт всё в одну)
            int partition = (i - 1) % 5;
            kafkaTemplate.send(TOPIC_5P, partition, null, value);
            sentMessages.add(Map.of("number", i, "value", value));
        }
        kafkaTemplate.flush();

        List<Map<String, Object>> result1p = readFromBeginning(TOPIC_1P, count);
        List<Map<String, Object>> result5p = readFromBeginning(TOPIC_5P, count);

        Map<Integer, List<Map<String, Object>>> byPartition1p = groupByPartition(result1p);
        Map<Integer, List<Map<String, Object>>> byPartition5p = groupByPartition(result5p);

        boolean ordered1p = isOrdered(result1p);
        boolean orderedGlobal5p = isOrdered(result5p);
        boolean orderedPerPartition = byPartition5p.values().stream().allMatch(this::isOrdered);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sent", sentMessages);
        result.put("count", count);
        result.put("result1p", result1p);
        result.put("result5p", result5p);
        result.put("byPartition1p", byPartition1p);
        result.put("byPartition5p", byPartition5p);
        result.put("ordered1p", ordered1p);
        result.put("orderedGlobal5p", orderedGlobal5p);
        result.put("orderedPerPartition", orderedPerPartition);

        lastResult = result;
        return result;
    }

    /**
     * Демо 2: С ключами — каждый ключ всегда в одной партиции.
     * Порядок внутри ключа ГАРАНТИРОВАН.
     */
    public Map<String, Object> runMultiKeyDemo(int countPerKey, List<String> keys) {
        resetTopics();

        int totalExpected = countPerKey * keys.size();
        List<Map<String, Object>> sentMessages = new ArrayList<>();
        for (int round = 1; round <= countPerKey; round++) {
            for (String key : keys) {
                String value = key + " #" + round;
                kafkaTemplate.send(TOPIC_5P, key, value);
                sentMessages.add(Map.of("number", round, "key", key, "value", value));
            }
        }
        kafkaTemplate.flush();

        List<Map<String, Object>> result5p = readFromBeginning(TOPIC_5P, totalExpected);

        Map<Integer, List<Map<String, Object>>> byPartition = groupByPartition(result5p);

        Map<String, List<Map<String, Object>>> byKey = new LinkedHashMap<>();
        for (Map<String, Object> msg : result5p) {
            String k = (String) msg.get("key");
            byKey.computeIfAbsent(k, x -> new ArrayList<>()).add(msg);
        }

        // Какой ключ в какую партицию попал
        Map<String, Integer> keyToPartition = new LinkedHashMap<>();
        for (Map<String, Object> msg : result5p) {
            String k = (String) msg.get("key");
            if (!keyToPartition.containsKey(k)) {
                keyToPartition.put(k, (int) msg.get("partition"));
            }
        }

        boolean orderedPerKey = byKey.values().stream().allMatch(this::isOrdered);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sent", sentMessages);
        result.put("keys", keys);
        result.put("countPerKey", countPerKey);
        result.put("result5p", result5p);
        result.put("byPartition", byPartition);
        result.put("byKey", byKey);
        result.put("keyToPartition", keyToPartition);
        result.put("orderedPerKey", orderedPerKey);

        lastResult = result;
        return result;
    }

    private void resetTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existing = admin.listTopics().names().get();

            List<String> toDelete = new ArrayList<>();
            if (existing.contains(TOPIC_1P)) toDelete.add(TOPIC_1P);
            if (existing.contains(TOPIC_5P)) toDelete.add(TOPIC_5P);
            if (!toDelete.isEmpty()) {
                admin.deleteTopics(toDelete).all().get();
                Thread.sleep(500);
            }

            admin.createTopics(List.of(
                    new NewTopic(TOPIC_1P, 1, (short) 1),
                    new NewTopic(TOPIC_5P, 5, (short) 1)
            )).all().get();
            Thread.sleep(500);

        } catch (Exception e) {
            log.warn("Failed to reset topics: {}", e.getMessage());
        }
    }

    private List<Map<String, Object>> readFromBeginning(String topic, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        List<Map<String, Object>> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            long deadline = System.currentTimeMillis() + 3000;
            while (messages.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> r : records) {
                    int number = extractNumber(r.value());
                    Map<String, Object> msg = new LinkedHashMap<>();
                    msg.put("number", number);
                    msg.put("key", r.key() != null ? r.key() : "");
                    msg.put("value", r.value());
                    msg.put("partition", r.partition());
                    msg.put("offset", r.offset());
                    messages.add(msg);
                }
            }
        }

        return messages;
    }

    private Map<Integer, List<Map<String, Object>>> groupByPartition(List<Map<String, Object>> messages) {
        Map<Integer, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (Map<String, Object> msg : messages) {
            int p = (int) msg.get("partition");
            result.computeIfAbsent(p, k -> new ArrayList<>()).add(msg);
        }
        return result;
    }

    private int extractNumber(String value) {
        if (value == null) return 0;
        int idx = value.lastIndexOf('#');
        if (idx >= 0 && idx < value.length() - 1) {
            try {
                return Integer.parseInt(value.substring(idx + 1).trim());
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    private boolean isOrdered(List<Map<String, Object>> messages) {
        for (int i = 1; i < messages.size(); i++) {
            int prev = (int) messages.get(i - 1).get("number");
            int curr = (int) messages.get(i).get("number");
            if (curr < prev) return false;
        }
        return true;
    }
}
