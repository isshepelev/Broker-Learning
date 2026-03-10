package manufacture.ru.brokerlearning.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class ScheduledProducerJob {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MessageHistoryService messageHistoryService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<Map<String, String>> recentMessages = Collections.synchronizedList(new ArrayList<>());

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Scheduled(fixedRate = 3000)
    public void produceMetrics() {
        if (!running.get()) {
            return;
        }

        int count = counter.incrementAndGet();
        double cpuValue = Math.round(Math.random() * 10000.0) / 100.0;
        String metric = String.format("{\"metric\":\"cpu_usage\",\"value\":%.2f,\"timestamp\":\"%s\",\"seq\":%d}",
                cpuValue, Instant.now(), count);

        kafkaTemplate.send("metrics-topic", "metric-" + count, metric);
        messageHistoryService.saveSentMessage("metrics-topic", "metric-" + count, metric, null, null);

        String time = LocalDateTime.now().format(TIME_FMT);
        recentMessages.add(0, Map.of("time", time, "key", "metric-" + count, "value", "cpu_usage=" + cpuValue + "%", "seq", String.valueOf(count)));
        while (recentMessages.size() > 50) {
            recentMessages.remove(recentMessages.size() - 1);
        }

        log.info("Produced metric #{}: {}", count, metric);
    }

    public void start() {
        running.set(true);
        log.info("Scheduled producer job started");
    }

    public void stop() {
        running.set(false);
        log.info("Scheduled producer job stopped");
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
