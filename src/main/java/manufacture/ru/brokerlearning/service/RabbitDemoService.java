package manufacture.ru.brokerlearning.service;

import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.RabbitConfig;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class RabbitDemoService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String SID_SEPARATOR = "::";

    private final RabbitTemplate rabbitTemplate;

    private static class RabbitSession {
        final List<Map<String, String>> sentMessages = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, String>> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger sentCount = new AtomicInteger(0);
        final AtomicInteger receivedCount = new AtomicInteger(0);
    }

    private final ConcurrentHashMap<String, RabbitSession> sessions = new ConcurrentHashMap<>();

    public RabbitDemoService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    private RabbitSession session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new RabbitSession());
    }

    public void send(String sid, String message) {
        RabbitSession s = session(sid);
        String time = LocalDateTime.now().format(FMT);
        // Embed sid in message for routing in listener
        rabbitTemplate.convertAndSend(RabbitConfig.DEMO_EXCHANGE, RabbitConfig.DEMO_ROUTING_KEY, sid + SID_SEPARATOR + message);
        s.sentCount.incrementAndGet();
        s.sentMessages.add(0, Map.of("time", time, "value", message));
        while (s.sentMessages.size() > 50) s.sentMessages.remove(s.sentMessages.size() - 1);
    }

    @RabbitListener(queues = RabbitConfig.DEMO_QUEUE)
    public void receive(String rawMessage) {
        String sid = extractSid(rawMessage);
        String message = extractMessage(rawMessage);
        RabbitSession s = session(sid);
        String time = LocalDateTime.now().format(FMT);
        s.receivedCount.incrementAndGet();
        s.receivedMessages.add(0, Map.of("time", time, "value", message));
        while (s.receivedMessages.size() > 50) s.receivedMessages.remove(s.receivedMessages.size() - 1);
    }

    public List<Map<String, String>> getSentMessages(String sid) { return session(sid).sentMessages; }
    public List<Map<String, String>> getReceivedMessages(String sid) { return session(sid).receivedMessages; }
    public int getSentCount(String sid) { return session(sid).sentCount.get(); }
    public int getReceivedCount(String sid) { return session(sid).receivedCount.get(); }

    public void clear(String sid) {
        sessions.remove(sid);
    }

    private String extractSid(String raw) {
        if (raw != null && raw.contains(SID_SEPARATOR)) return raw.substring(0, raw.indexOf(SID_SEPARATOR));
        return "_default";
    }

    private String extractMessage(String raw) {
        if (raw != null && raw.contains(SID_SEPARATOR)) return raw.substring(raw.indexOf(SID_SEPARATOR) + SID_SEPARATOR.length());
        return raw;
    }
}
