package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class DynamicConsumerService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final int MAX_MESSAGES = 500;

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AtomicInteger counter = new AtomicInteger(0);

    private final Map<String, ConsumerHandle> consumers = new ConcurrentHashMap<>();

    public DynamicConsumerService(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
    }

    public Map<String, Object> createConsumer(String name, String topic, String groupId, String ownerSid) {
        if (name == null || name.isBlank()) {
            name = "consumer-" + counter.incrementAndGet();
        }
        if (groupId == null || groupId.isBlank()) {
            groupId = "dynamic-" + name;
        }

        if (consumers.containsKey(name)) {
            return Map.of("success", false, "error", "Consumer с именем '" + name + "' уже существует");
        }

        ConsumerHandle handle = new ConsumerHandle(name, topic, groupId, ownerSid);
        consumers.put(name, handle);
        handle.future = executor.submit(() -> runConsumer(handle));

        log.info("Dynamic consumer '{}' created for topic '{}', group '{}', owner '{}'", name, topic, groupId, ownerSid);
        return Map.of("success", true, "consumer", getConsumerInfo(handle));
    }

    public Map<String, Object> stopConsumer(String name) {
        ConsumerHandle handle = consumers.get(name);
        if (handle == null) {
            return Map.of("success", false, "error", "Consumer '" + name + "' не найден");
        }

        handle.running = false;
        if (handle.consumer != null) {
            handle.consumer.wakeup();
        }
        if (handle.future != null) {
            try {
                handle.future.get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
        consumers.remove(name);

        log.info("Dynamic consumer '{}' stopped", name);
        return Map.of("success", true);
    }

    public Map<String, Object> stopAll() {
        for (ConsumerHandle handle : consumers.values()) {
            handle.running = false;
            if (handle.consumer != null) handle.consumer.wakeup();
        }
        for (ConsumerHandle handle : consumers.values()) {
            if (handle.future != null) {
                try {
                    handle.future.get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
            }
        }
        consumers.clear();
        counter.set(0);
        return Map.of("success", true);
    }

    public Map<String, Object> stopAllForUser(String ownerSid) {
        List<String> toStop = new ArrayList<>();
        for (var entry : consumers.entrySet()) {
            if (ownerSid.equals(entry.getValue().ownerSid)) {
                toStop.add(entry.getKey());
            }
        }
        for (String name : toStop) {
            stopConsumer(name);
        }
        return Map.of("success", true, "stopped", toStop.size());
    }

    /** List consumers owned by a specific user */
    public List<Map<String, Object>> listConsumers(String ownerSid) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (ConsumerHandle handle : consumers.values()) {
            if (ownerSid.equals(handle.ownerSid)) {
                result.add(getConsumerInfo(handle));
            }
        }
        result.sort(Comparator.comparing(m -> (String) m.get("name")));
        return result;
    }

    /** List all consumers (for backward compatibility) */
    public List<Map<String, Object>> listConsumers() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (ConsumerHandle handle : consumers.values()) {
            result.add(getConsumerInfo(handle));
        }
        result.sort(Comparator.comparing(m -> (String) m.get("name")));
        return result;
    }

    public List<Map<String, Object>> getMessages(String consumerName, Integer partition) {
        ConsumerHandle handle = consumers.get(consumerName);
        if (handle == null) return List.of();

        List<Map<String, Object>> result = new ArrayList<>();
        synchronized (handle.messages) {
            for (MessageRecord msg : handle.messages) {
                if (partition != null && msg.partition != partition) continue;
                result.add(msg.toMap());
            }
        }
        return result;
    }

    public List<Map<String, Object>> getAllMessages(String ownerSid) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (ConsumerHandle handle : consumers.values()) {
            if (!ownerSid.equals(handle.ownerSid)) continue;
            synchronized (handle.messages) {
                for (MessageRecord msg : handle.messages) {
                    Map<String, Object> m = msg.toMap();
                    m.put("consumerName", handle.name);
                    result.add(m);
                }
            }
        }
        result.sort((a, b) -> ((String) b.get("timestamp")).compareTo((String) a.get("timestamp")));
        return result;
    }

    public Set<Integer> getPartitions(String consumerName) {
        ConsumerHandle handle = consumers.get(consumerName);
        if (handle == null) return Set.of();
        return new TreeSet<>(handle.assignedPartitions);
    }

    private Map<String, Object> getConsumerInfo(ConsumerHandle handle) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", handle.name);
        info.put("topic", handle.topic);
        info.put("groupId", handle.groupId);
        info.put("running", handle.running);
        info.put("messageCount", handle.messages.size());
        info.put("assignedPartitions", new TreeSet<>(handle.assignedPartitions));
        info.put("createdAt", handle.createdAt.format(FMT));
        return info;
    }

    private void runConsumer(ConsumerHandle handle) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, handle.groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            handle.consumer = consumer;

            consumer.subscribe(List.of(handle.topic), new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
                    handle.assignedPartitions.clear();
                }
                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
                    handle.assignedPartitions.clear();
                    partitions.forEach(tp -> handle.assignedPartitions.add(tp.partition()));
                }
            });

            while (handle.running) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        MessageRecord msg = new MessageRecord(
                                record.topic(), record.key(), record.value(),
                                record.partition(), record.offset(), LocalDateTime.now()
                        );
                        synchronized (handle.messages) {
                            handle.messages.addFirst(msg);
                            while (handle.messages.size() > MAX_MESSAGES) handle.messages.removeLast();
                        }
                    }
                } catch (WakeupException e) {
                    if (!handle.running) break;
                }
            }
        } catch (Exception e) {
            log.error("Consumer '{}' error: {}", handle.name, e.getMessage(), e);
        } finally {
            handle.running = false;
        }
    }

    private static class ConsumerHandle {
        final String name;
        final String topic;
        final String groupId;
        final String ownerSid;
        final LocalDateTime createdAt = LocalDateTime.now();
        final LinkedList<MessageRecord> messages = new LinkedList<>();
        final Set<Integer> assignedPartitions = ConcurrentHashMap.newKeySet();
        volatile boolean running = true;
        volatile KafkaConsumer<String, String> consumer;
        volatile Future<?> future;

        ConsumerHandle(String name, String topic, String groupId, String ownerSid) {
            this.name = name;
            this.topic = topic;
            this.groupId = groupId;
            this.ownerSid = ownerSid;
        }
    }

    private static class MessageRecord {
        final String topic, key, value;
        final int partition;
        final long offset;
        final LocalDateTime time;

        MessageRecord(String topic, String key, String value, int partition, long offset, LocalDateTime time) {
            this.topic = topic; this.key = key; this.value = value;
            this.partition = partition; this.offset = offset; this.time = time;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("topic", topic);
            m.put("key", key != null ? key : "");
            m.put("value", value != null ? value : "");
            m.put("partition", partition);
            m.put("offset", offset);
            m.put("timestamp", time.format(FMT));
            return m;
        }
    }
}
