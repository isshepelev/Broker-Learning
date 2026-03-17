package manufacture.ru.brokerlearning.job;
import manufacture.ru.brokerlearning.config.UserSessionHelper;

import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class ScheduledProducerJob {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MessageHistoryService messageHistoryService;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static class ProducerState {
        final AtomicBoolean running = new AtomicBoolean(false);
        final AtomicInteger counter = new AtomicInteger(0);
        final List<Map<String, String>> recentMessages = Collections.synchronizedList(new ArrayList<>());
    }

    private final ConcurrentHashMap<String, ProducerState> sessions = new ConcurrentHashMap<>();

    public ScheduledProducerJob(KafkaTemplate<String, String> kafkaTemplate,
                                MessageHistoryService messageHistoryService) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageHistoryService = messageHistoryService;
    }

    private ProducerState session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new ProducerState());
    }

    @Scheduled(fixedRate = 3000)
    public void produceMetrics() {
        sessions.forEach((sid, s) -> {
            if (!s.running.get()) return;

            int count = s.counter.incrementAndGet();
            double cpuValue = Math.round(Math.random() * 10000.0) / 100.0;
            String metric = String.format("{\"metric\":\"cpu_usage\",\"value\":%.2f,\"timestamp\":\"%s\",\"seq\":%d}",
                    cpuValue, Instant.now(), count);
            String topic = UserSessionHelper.isAdminSid(sid) ? "metrics-topic" : "metrics-topic-" + sid;

            kafkaTemplate.send(topic, "metric-" + count, metric);
            messageHistoryService.saveSentMessage(topic, "metric-" + count, metric, null, null, sid);

            String time = LocalDateTime.now().format(TIME_FMT);
            s.recentMessages.add(0, Map.of("time", time, "key", "metric-" + count, "value", "cpu_usage=" + cpuValue + "%", "seq", String.valueOf(count)));
            while (s.recentMessages.size() > 50) s.recentMessages.remove(s.recentMessages.size() - 1);
        });
    }

    public void start(String sid) { session(sid).running.set(true); }
    public void stop(String sid) { session(sid).running.set(false); }
    public boolean isRunning(String sid) { return session(sid).running.get(); }
    public int getCounter(String sid) { return session(sid).counter.get(); }
    public List<Map<String, String>> getRecentMessages(String sid) { return new ArrayList<>(session(sid).recentMessages); }
    public void cleanup(String sid) { sessions.remove(sid); }
}
