package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@Slf4j
public class RealtimeMessageService {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(5 * 60 * 1000L);

        emitters.add(emitter);
        log.info("New SSE subscriber added. Total subscribers: {}", emitters.size());

        emitter.onCompletion(() -> {
            emitters.remove(emitter);
            log.info("SSE subscriber completed. Total subscribers: {}", emitters.size());
        });

        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            log.info("SSE subscriber timed out. Total subscribers: {}", emitters.size());
        });

        emitter.onError(e -> {
            emitters.remove(emitter);
            log.warn("SSE subscriber error: {}. Total subscribers: {}", e.getMessage(), emitters.size());
        });

        return emitter;
    }

    public void broadcast(ConsumerRecord<String, String> record) {
        String message = String.format(
                "{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\",\"timestamp\":%d}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() != null ? record.key() : "",
                record.value() != null ? record.value().replace("\"", "\\\"") : "",
                record.timestamp()
        );

        log.debug("Broadcasting message to {} subscribers: {}", emitters.size(), message);

        List<SseEmitter> deadEmitters = new ArrayList<>();

        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("kafka-message")
                        .data(message));
            } catch (IOException e) {
                log.warn("Failed to send SSE event, removing emitter: {}", e.getMessage());
                deadEmitters.add(emitter);
            }
        }

        emitters.removeAll(deadEmitters);
    }
}
