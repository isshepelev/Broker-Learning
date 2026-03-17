package manufacture.ru.brokerlearning.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class ScheduledConsumerJob {

    private final String bootstrapServers;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static class ConsumerState {
        final AtomicBoolean running = new AtomicBoolean(false);
        final AtomicInteger counter = new AtomicInteger(0);
        final List<Map<String, String>> recentMessages = Collections.synchronizedList(new ArrayList<>());
        volatile KafkaConsumer<String, String> consumer;
        volatile Future<?> future;
    }

    private final ConcurrentHashMap<String, ConsumerState> sessions = new ConcurrentHashMap<>();

    public ScheduledConsumerJob(org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        Object bs = kafkaAdmin.getConfigurationProperties().get("bootstrap.servers");
        this.bootstrapServers = bs instanceof String ? (String) bs
                : String.join(",", (Collection<String>) bs);
    }

    private ConsumerState session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new ConsumerState());
    }

    public void start(String sid) {
        ConsumerState s = session(sid);
        if (s.running.compareAndSet(false, true)) {
            s.future = executor.submit(() -> pollLoop(sid, s));
            log.info("Job consumer started for sid={}", sid);
        }
    }

    public void stop(String sid) {
        ConsumerState s = session(sid);
        s.running.set(false);
        if (s.consumer != null) s.consumer.wakeup();
        if (s.future != null) {
            try { s.future.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
        log.info("Job consumer stopped for sid={}", sid);
    }

    public boolean isRunning(String sid) { return session(sid).running.get(); }
    public int getCounter(String sid) { return session(sid).counter.get(); }
    public List<Map<String, String>> getRecentMessages(String sid) { return new ArrayList<>(session(sid).recentMessages); }

    public void cleanup(String sid) {
        stop(sid);
        sessions.remove(sid);
    }

    private void pollLoop(String sid, ConsumerState s) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "job-consumer-group-" + sid);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            s.consumer = consumer;
            consumer.subscribe(List.of("metrics-topic-" + sid));

            while (s.running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        int count = s.counter.incrementAndGet();
                        String time = LocalDateTime.now().format(TIME_FMT);
                        String shortValue = record.value() != null && record.value().length() > 60
                                ? record.value().substring(0, 60) + "..." : (record.value() != null ? record.value() : "-");
                        s.recentMessages.add(0, Map.of(
                                "time", time,
                                "key", record.key() != null ? record.key() : "-",
                                "value", shortValue,
                                "partition", String.valueOf(record.partition()),
                                "offset", String.valueOf(record.offset())
                        ));
                        while (s.recentMessages.size() > 50) s.recentMessages.remove(s.recentMessages.size() - 1);
                    }
                } catch (WakeupException e) {
                    if (!s.running.get()) break;
                }
            }
        } catch (Exception e) {
            log.error("Job consumer error for sid={}: {}", sid, e.getMessage());
        } finally {
            s.running.set(false);
            s.consumer = null;
        }
    }
}
