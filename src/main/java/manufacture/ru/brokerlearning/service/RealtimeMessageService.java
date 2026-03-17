package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@Slf4j
public class RealtimeMessageService {

    /** Per-user SSE emitters: sid → list of emitters */
    private final ConcurrentHashMap<String, List<SseEmitter>> emittersBySid = new ConcurrentHashMap<>();

    public SseEmitter subscribe(String sid) {
        SseEmitter emitter = new SseEmitter(5 * 60 * 1000L);

        List<SseEmitter> list = emittersBySid.computeIfAbsent(sid, k -> new CopyOnWriteArrayList<>());
        list.add(emitter);

        emitter.onCompletion(() -> list.remove(emitter));
        emitter.onTimeout(() -> list.remove(emitter));
        emitter.onError(e -> list.remove(emitter));

        return emitter;
    }

    /** Broadcast to a specific user's SSE subscribers */
    public void broadcast(ConsumerRecord<String, String> record, String ownerSid) {
        if (ownerSid == null) return;

        List<SseEmitter> list = emittersBySid.get(ownerSid);
        if (list == null || list.isEmpty()) return;

        String message = String.format(
                "{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\",\"value\":\"%s\",\"timestamp\":%d}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() != null ? record.key() : "",
                record.value() != null ? record.value().replace("\"", "\\\"") : "",
                record.timestamp()
        );

        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().name("kafka-message").data(message));
            } catch (IOException e) {
                dead.add(emitter);
            }
        }
        list.removeAll(dead);
    }
}
