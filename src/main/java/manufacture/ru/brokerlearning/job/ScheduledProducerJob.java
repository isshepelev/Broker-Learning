package manufacture.ru.brokerlearning.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class ScheduledProducerJob {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger counter = new AtomicInteger(0);

    @Scheduled(fixedRate = 5000)
    public void produceMetrics() {
        if (!running.get()) {
            return;
        }

        int count = counter.incrementAndGet();
        String metric = String.format("{\"metric\": \"cpu_usage\", \"value\": %.2f, \"timestamp\": \"%s\", \"seq\": %d}",
                Math.random() * 100, Instant.now(), count);

        kafkaTemplate.send("metrics-topic", metric);
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
}
