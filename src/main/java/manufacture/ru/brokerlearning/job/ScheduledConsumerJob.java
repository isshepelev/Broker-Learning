package manufacture.ru.brokerlearning.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class ScheduledConsumerJob {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<String> lastMessages = Collections.synchronizedList(new ArrayList<>());

    @Scheduled(fixedDelay = 10000)
    public void pollMessages() {
        if (!running.get()) {
            return;
        }

        // Simulated poll — actual Kafka consumers use @KafkaListener
        String simulatedMessage = String.format("Simulated poll at %s — in production, use @KafkaListener for real consumption", Instant.now());
        lastMessages.add(simulatedMessage);

        // Keep only the last 100 messages
        while (lastMessages.size() > 100) {
            lastMessages.remove(0);
        }

        log.info("Scheduled consumer poll: {}", simulatedMessage);
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

    public List<String> getLastMessages() {
        return Collections.unmodifiableList(new ArrayList<>(lastMessages));
    }
}
