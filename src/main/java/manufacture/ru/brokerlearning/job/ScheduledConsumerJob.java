package manufacture.ru.brokerlearning.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job consumer для metrics-topic.
 * НЕ сохраняет и НЕ транслирует в SSE — это делает главный SimpleConsumer (topicPattern=".*").
 * Только ведёт свой лог для отображения на странице Jobs.
 */
@Service
@Slf4j
public class ScheduledConsumerJob {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<Map<String, String>> recentMessages = Collections.synchronizedList(new ArrayList<>());

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    @KafkaListener(topics = "metrics-topic", groupId = "job-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (!running.get()) {
            ack.acknowledge();
            return;
        }

        int count = counter.incrementAndGet();

        String time = LocalDateTime.now().format(TIME_FMT);
        String shortValue = record.value() != null && record.value().length() > 60
                ? record.value().substring(0, 60) + "..." : (record.value() != null ? record.value() : "-");
        recentMessages.add(0, Map.of(
                "time", time,
                "key", record.key() != null ? record.key() : "-",
                "value", shortValue,
                "partition", String.valueOf(record.partition()),
                "offset", String.valueOf(record.offset())
        ));
        while (recentMessages.size() > 50) {
            recentMessages.remove(recentMessages.size() - 1);
        }

        ack.acknowledge();
        log.info("Job consumer received #{}: key={}, partition={}, offset={}", count, record.key(), record.partition(), record.offset());
    }

    public void start() {
        running.set(true);
        log.info("Scheduled consumer job started");
    }

    public void stop() {
        running.set(false);
        log.info("Scheduled consumer job stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    public int getCounter() {
        return counter.get();
    }

    public List<Map<String, String>> getRecentMessages() {
        return new ArrayList<>(recentMessages);
    }
}
