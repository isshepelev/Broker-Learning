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

/**
 * Режим практики: пользователь сам настраивает топик, партиции и consumer-группу.
 */
@Service
@Slf4j
public class RebalancingPracticeService {

    private static final int MAX_PARTITIONS = 20;
    private static final int MAX_CONSUMERS = 10;
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    // Текущая конфигурация практики
    private volatile String currentTopic;
    private volatile String currentGroupId;
    private volatile int currentPartitions;
    private volatile boolean sessionActive = false;

    private final AtomicInteger consumerCounter = new AtomicInteger(0);
    private final Map<String, PracticeConsumerHandle> handles = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> assignments = new ConcurrentHashMap<>();
    private final List<Map<String, String>> eventLog = Collections.synchronizedList(new ArrayList<>());

    private static class PracticeConsumerHandle {
        volatile Future<?> future;
        volatile KafkaConsumer<String, String> consumer;
        volatile boolean running = true;
    }

    public RebalancingPracticeService(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
    }

    /**
     * Создать сессию практики: топик + группу.
     */
    public synchronized Map<String, Object> createSession(String topicName, String groupId, int partitions) {
        // Сначала останавливаем предыдущую сессию если есть
        if (sessionActive) {
            doReset();
        }

        topicName = topicName.trim();
        groupId = groupId.trim();

        if (topicName.isEmpty() || groupId.isEmpty()) {
            return Map.of("error", "Имя топика и группы не могут быть пустыми");
        }
        if (partitions < 1 || partitions > MAX_PARTITIONS) {
            return Map.of("error", "Количество партиций: от 1 до " + MAX_PARTITIONS);
        }

        // Создаём/проверяем топик
        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            if (existing.contains(topicName)) {
                // Топик существует — узнаём количество партиций
                var desc = admin.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName);
                int existingPartitions = desc.partitions().size();
                if (partitions > existingPartitions) {
                    // Увеличиваем
                    admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(partitions))).all().get();
                    currentPartitions = partitions;
                } else {
                    currentPartitions = existingPartitions;
                }
                addEvent("SESSION", "—", "Используется существующий топик «" + topicName + "» (" + currentPartitions + " партиций)");
            } else {
                admin.createTopics(List.of(new NewTopic(topicName, partitions, (short) 1))).all().get();
                currentPartitions = partitions;
                addEvent("SESSION", "—", "Создан топик «" + topicName + "» с " + partitions + " партициями");
            }
        } catch (Exception e) {
            log.error("Practice: create session error: {}", e.getMessage());
            return Map.of("error", "Ошибка создания топика: " + e.getMessage());
        }

        this.currentTopic = topicName;
        this.currentGroupId = groupId;
        this.sessionActive = true;
        this.consumerCounter.set(0);

        addEvent("SESSION", "—", "Группа: «" + groupId + "», готово к добавлению consumer'ов");
        log.info("Practice: session started — topic={}, group={}, partitions={}", topicName, groupId, currentPartitions);

        return getStatus();
    }

    public synchronized Map<String, Object> addConsumer() {
        if (!sessionActive) return Map.of("error", "Сначала создайте сессию");
        if (handles.size() >= MAX_CONSUMERS) {
            addEvent("ADD", "—", "Достигнут лимит: " + MAX_CONSUMERS + " consumer'ов");
            return getStatus();
        }

        String id = "Consumer-" + consumerCounter.incrementAndGet();
        PracticeConsumerHandle handle = new PracticeConsumerHandle();
        handles.put(id, handle);
        handle.future = executor.submit(() -> runConsumer(id, handle));

        addEvent("ADD", id, "Добавлен в группу «" + currentGroupId + "», ожидание rebalance...");
        log.info("Practice: adding {} to group {}", id, currentGroupId);

        waitForStableAssignments();
        return getStatus();
    }

    public synchronized Map<String, Object> removeConsumer() {
        if (!sessionActive || handles.isEmpty()) return getStatus();

        String lastId = handles.keySet().stream()
                .max(Comparator.comparingInt(k -> Integer.parseInt(k.split("-")[1])))
                .orElse(null);
        if (lastId == null) return getStatus();

        PracticeConsumerHandle handle = handles.remove(lastId);
        handle.running = false;
        if (handle.consumer != null) handle.consumer.wakeup();
        if (handle.future != null) {
            try { handle.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
        assignments.remove(lastId);

        addEvent("REMOVE", lastId, "Удалён из группы, запущен rebalance...");
        log.info("Practice: removed {} from group {}", lastId, currentGroupId);

        if (!handles.isEmpty()) {
            waitForStableAssignments();
        }
        return getStatus();
    }

    public synchronized Map<String, Object> addPartition() {
        if (!sessionActive) return Map.of("error", "Сначала создайте сессию");
        if (currentPartitions >= MAX_PARTITIONS) {
            addEvent("PARTITION", "—", "Достигнут лимит: " + MAX_PARTITIONS + " партиций");
            return getStatus();
        }

        int newCount = currentPartitions + 1;
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createPartitions(Map.of(currentTopic, NewPartitions.increaseTo(newCount))).all().get();
            currentPartitions = newCount;
            addEvent("PARTITION", "—", "Добавлена партиция P" + (newCount - 1) + " (всего: " + newCount + ")");
            log.info("Practice: increased partitions to {} for topic {}", newCount, currentTopic);
        } catch (Exception e) {
            log.error("Practice: add partition error: {}", e.getMessage());
            addEvent("PARTITION", "—", "Ошибка: " + e.getMessage());
            return getStatus();
        }

        if (!handles.isEmpty()) {
            waitForStableAssignments();
        }
        return getStatus();
    }

    public synchronized Map<String, Object> reset() {
        doReset();
        return getStatus();
    }

    private void doReset() {
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
        consumerCounter.set(0);
        sessionActive = false;
        currentTopic = null;
        currentGroupId = null;
        currentPartitions = 0;

        addEvent("RESET", "—", "Сессия практики сброшена");
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("sessionActive", sessionActive);
        status.put("topicName", currentTopic);
        status.put("groupId", currentGroupId);
        status.put("totalPartitions", currentPartitions);
        status.put("maxPartitions", MAX_PARTITIONS);
        status.put("consumerCount", handles.size());
        status.put("maxConsumers", MAX_CONSUMERS);
        status.put("assignments", new LinkedHashMap<>(assignments));
        status.put("events", new ArrayList<>(eventLog));
        status.put("allConsumers", new ArrayList<>(handles.keySet()));

        Set<Integer> allAssigned = new HashSet<>();
        assignments.values().forEach(allAssigned::addAll);
        Set<Integer> unassigned = new LinkedHashSet<>();
        for (int i = 0; i < currentPartitions; i++) {
            if (!allAssigned.contains(i)) unassigned.add(i);
        }
        status.put("unassigned", unassigned);

        return status;
    }

    /**
     * Список существующих топиков для выбора.
     */
    public Set<String> listTopics() {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            return admin.listTopics().names().get();
        } catch (Exception e) {
            log.error("Practice: list topics error: {}", e.getMessage());
            return Set.of();
        }
    }

    // ───── internal ─────

    private void runConsumer(String consumerId, PracticeConsumerHandle handle) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, currentGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "60000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            handle.consumer = consumer;

            consumer.subscribe(List.of(currentTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
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

            while (handle.running) {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                } catch (WakeupException e) {
                    if (!handle.running) break;
                }
            }
        } catch (Exception e) {
            log.warn("Practice: {} error: {}", consumerId, e.getMessage());
        } finally {
            log.info("Practice: {} shut down", consumerId);
        }
    }

    private void waitForStableAssignments() {
        long deadline = System.currentTimeMillis() + 15_000;
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        while (System.currentTimeMillis() < deadline) {
            if (isAssignmentStable()) {
                log.info("Practice: assignments stable — {}", assignments);
                return;
            }
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        }
        log.warn("Practice: timed out waiting for stable assignments");
    }

    private boolean isAssignmentStable() {
        Set<Integer> allPartitions = new HashSet<>();
        for (String cId : handles.keySet()) {
            Set<Integer> parts = assignments.get(cId);
            if (parts != null) allPartitions.addAll(parts);
        }
        return allPartitions.size() == currentPartitions;
    }

    private void addEvent(String type, String consumer, String message) {
        synchronized (eventLog) {
            eventLog.add(0, Map.of(
                    "time", LocalTime.now().format(FMT),
                    "type", type,
                    "consumer", consumer,
                    "message", message
            ));
            while (eventLog.size() > 50) eventLog.remove(eventLog.size() - 1);
        }
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        return props;
    }
}
