package manufacture.ru.brokerlearning.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

/**
 * Демо Consumer Group Rebalancing с реальными Kafka consumer'ами.
 */
@Service
@Slf4j
public class RebalancingService {

    private static final String TOPIC = "rebalancing-topic";
    private static final String GROUP_ID = "rebalancing-demo-group";
    private static final int PARTITIONS = 6;
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AtomicInteger counter = new AtomicInteger(0);

    // Consumer state
    private final Map<String, ConsumerHandle> handles = new ConcurrentHashMap<>();

    // Текущее назначение: consumerId → partitions
    @Getter
    private final Map<String, Set<Integer>> assignments = new ConcurrentHashMap<>();

    // Лог событий
    @Getter
    private final List<Map<String, String>> eventLog = Collections.synchronizedList(new ArrayList<>());

    private static class ConsumerHandle {
        volatile Future<?> future;
        volatile KafkaConsumer<String, String> consumer;
        volatile boolean running = true;
    }

    public RebalancingService(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
        ensureTopic();
    }

    public synchronized Map<String, Object> addConsumer() {
        String id = "Consumer-" + counter.incrementAndGet();

        ConsumerHandle handle = new ConsumerHandle();
        handles.put(id, handle);
        handle.future = executor.submit(() -> runConsumer(id, handle));

        addEvent("ADD", id, "Добавлен в группу, ожидание rebalance...");
        log.info("Rebalancing: adding {}", id);

        waitForStableAssignments();

        return getStatus();
    }

    public synchronized Map<String, Object> removeConsumer() {
        if (handles.isEmpty()) return getStatus();

        // Удаляем последнего
        String lastId = handles.keySet().stream()
                .max(Comparator.comparingInt(k -> Integer.parseInt(k.split("-")[1])))
                .orElse(null);
        if (lastId == null) return getStatus();

        ConsumerHandle handle = handles.remove(lastId);

        // Помечаем что consumer должен остановиться и wakeup
        handle.running = false;
        if (handle.consumer != null) {
            handle.consumer.wakeup();
        }
        // Ждём завершения потока
        if (handle.future != null) {
            try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
        assignments.remove(lastId);

        addEvent("REMOVE", lastId, "Удалён из группы, запущен rebalance...");
        log.info("Rebalancing: removed {}", lastId);

        // Ждём rebalance у оставшихся
        if (!handles.isEmpty()) {
            waitForStableAssignments();
        }

        return getStatus();
    }

    public synchronized Map<String, Object> reset() {
        // Останавливаем всех через wakeup
        for (var handle : handles.values()) {
            handle.running = false;
            if (handle.consumer != null) handle.consumer.wakeup();
        }
        for (var handle : handles.values()) {
            if (handle.future != null) {
                try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
            }
        }
        handles.clear();
        assignments.clear();
        eventLog.clear();
        counter.set(0);

        addEvent("RESET", "—", "Все consumer'ы остановлены");
        return getStatus();
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("totalPartitions", PARTITIONS);
        status.put("consumerCount", handles.size());
        status.put("assignments", new LinkedHashMap<>(assignments));
        status.put("events", new ArrayList<>(eventLog));
        status.put("allConsumers", new ArrayList<>(handles.keySet()));

        Set<Integer> allAssigned = new HashSet<>();
        assignments.values().forEach(allAssigned::addAll);
        Set<Integer> unassigned = new LinkedHashSet<>();
        for (int i = 0; i < PARTITIONS; i++) {
            if (!allAssigned.contains(i)) unassigned.add(i);
        }
        status.put("unassigned", unassigned);

        return status;
    }

    /**
     * Ждём пока все 6 партиций будут назначены consumer'ам (без дубликатов).
     * Polling вместо CountDownLatch — надёжнее при каскадных rebalance.
     */
    private void waitForStableAssignments() {
        long deadline = System.currentTimeMillis() + 15_000;
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        while (System.currentTimeMillis() < deadline) {
            if (isAssignmentStable()) {
                log.info("Rebalancing: assignments stable — {}", assignments);
                return;
            }
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        }
        log.warn("Rebalancing: timed out waiting for stable assignments, current: {}", assignments);
    }

    private boolean isAssignmentStable() {
        Set<Integer> allPartitions = new HashSet<>();
        for (String cId : handles.keySet()) {
            Set<Integer> parts = assignments.get(cId);
            if (parts != null) {
                allPartitions.addAll(parts);
            }
        }
        return allPartitions.size() == PARTITIONS;
    }

    private void runConsumer(String consumerId, ConsumerHandle handle) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            handle.consumer = consumer;

            consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Очищаем старые назначения чтобы не было стейлых данных
                    assignments.remove(consumerId);
                    if (!partitions.isEmpty()) {
                        Set<Integer> revoked = new LinkedHashSet<>();
                        partitions.forEach(tp -> revoked.add(tp.partition()));
                        addEvent("REVOKE", consumerId, "Отобраны партиции: " + revoked);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    Set<Integer> assigned = new LinkedHashSet<>();
                    partitions.forEach(tp -> assigned.add(tp.partition()));
                    assignments.put(consumerId, assigned);

                    if (!assigned.isEmpty()) {
                        addEvent("ASSIGN", consumerId, "Назначены партиции: " + assigned);
                    } else {
                        addEvent("ASSIGN", consumerId, "Нет партиций (idle)");
                    }
                }
            });

            // Poll loop — выход когда running = false
            while (handle.running) {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                } catch (WakeupException e) {
                    if (!handle.running) break;
                }
            }

        } catch (Exception e) {
            log.warn("Rebalancing: {} error: {}", consumerId, e.getMessage());
        } finally {
            log.info("Rebalancing: {} shut down", consumerId);
        }
    }

    private void addEvent(String type, String consumer, String message) {
        eventLog.add(0, Map.of(
                "time", LocalTime.now().format(FMT),
                "type", type,
                "consumer", consumer,
                "message", message
        ));
        while (eventLog.size() > 50) eventLog.remove(eventLog.size() - 1);
    }

    private void ensureTopic() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(TOPIC)) {
                admin.createTopics(List.of(
                        new org.apache.kafka.clients.admin.NewTopic(TOPIC, PARTITIONS, (short) 1)
                )).all().get();
            }
        } catch (Exception e) {
            log.warn("ensureTopic error: {}", e.getMessage());
        }
    }
}
