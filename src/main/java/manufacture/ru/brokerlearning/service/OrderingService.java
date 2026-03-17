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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderingService {

    private static final String TOPIC_1P_PREFIX = "ordering-1p-";
    private static final String TOPIC_5P_PREFIX = "ordering-5p-";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String bootstrapServers;

    private final ConcurrentHashMap<String, Map<String, Object>> lastResults = new ConcurrentHashMap<>();

    public OrderingService(KafkaTemplate<String, String> kafkaTemplate,
                           org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs : String.join(",", (java.util.Collection<String>) bs);
    }

    private String topic1p(String sid) { return TOPIC_1P_PREFIX + sid; }
    private String topic5p(String sid) { return TOPIC_5P_PREFIX + sid; }

    public Map<String, Object> getLastResult(String sid) {
        return lastResults.get(sid);
    }

    public Map<String, Object> runDemo(String sid, int count) {
        resetTopics(sid);

        List<Map<String, Object>> sentMessages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            String value = "Сообщение #" + i;
            kafkaTemplate.send(topic1p(sid), null, value);
            int partition = (i - 1) % 5;
            kafkaTemplate.send(topic5p(sid), partition, null, value);
            sentMessages.add(Map.of("number", i, "value", value));
        }
        kafkaTemplate.flush();

        List<Map<String, Object>> result1p = readFromBeginning(topic1p(sid), count);
        List<Map<String, Object>> result5p = readFromBeginning(topic5p(sid), count);

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

        lastResults.put(sid, result);
        return result;
    }

    public Map<String, Object> runMultiKeyDemo(String sid, int countPerKey, List<String> keys) {
        resetTopics(sid);

        int totalExpected = countPerKey * keys.size();
        List<Map<String, Object>> sentMessages = new ArrayList<>();
        for (int round = 1; round <= countPerKey; round++) {
            for (String key : keys) {
                String value = key + " #" + round;
                kafkaTemplate.send(topic5p(sid), key, value);
                sentMessages.add(Map.of("number", round, "key", key, "value", value));
            }
        }
        kafkaTemplate.flush();

        List<Map<String, Object>> result5p = readFromBeginning(topic5p(sid), totalExpected);
        Map<Integer, List<Map<String, Object>>> byPartition = groupByPartition(result5p);

        Map<String, List<Map<String, Object>>> byKey = new LinkedHashMap<>();
        for (Map<String, Object> msg : result5p) {
            String k = (String) msg.get("key");
            byKey.computeIfAbsent(k, x -> new ArrayList<>()).add(msg);
        }

        Map<String, Integer> keyToPartition = new LinkedHashMap<>();
        for (Map<String, Object> msg : result5p) {
            String k = (String) msg.get("key");
            if (!keyToPartition.containsKey(k)) keyToPartition.put(k, (int) msg.get("partition"));
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

        lastResults.put(sid, result);
        return result;
    }

    public void cleanupSession(String sid) {
        lastResults.remove(sid);
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            List<String> toDelete = List.of(topic1p(sid), topic5p(sid));
            admin.deleteTopics(toDelete).all().get();
        } catch (Exception ignored) {}
    }

    private void resetTopics(String sid) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existing = admin.listTopics().names().get();
            List<String> toDelete = new ArrayList<>();
            if (existing.contains(topic1p(sid))) toDelete.add(topic1p(sid));
            if (existing.contains(topic5p(sid))) toDelete.add(topic5p(sid));
            if (!toDelete.isEmpty()) {
                admin.deleteTopics(toDelete).all().get();
                Thread.sleep(500);
            }
            admin.createTopics(List.of(
                    new NewTopic(topic1p(sid), 1, (short) 1),
                    new NewTopic(topic5p(sid), 5, (short) 1)
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
            result.computeIfAbsent((int) msg.get("partition"), k -> new ArrayList<>()).add(msg);
        }
        return result;
    }

    private int extractNumber(String value) {
        if (value == null) return 0;
        int idx = value.lastIndexOf('#');
        if (idx >= 0 && idx < value.length() - 1) {
            try { return Integer.parseInt(value.substring(idx + 1).trim()); }
            catch (NumberFormatException e) { return 0; }
        }
        return 0;
    }

    private boolean isOrdered(List<Map<String, Object>> messages) {
        for (int i = 1; i < messages.size(); i++) {
            if ((int) messages.get(i).get("number") < (int) messages.get(i - 1).get("number")) return false;
        }
        return true;
    }
}
