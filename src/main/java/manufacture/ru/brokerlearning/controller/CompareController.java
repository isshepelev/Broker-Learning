package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.service.RabbitDemoService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
@RequestMapping("/compare")
@Slf4j
@RequiredArgsConstructor
public class CompareController {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String TOPIC_PREFIX = "compare-topic-";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RabbitDemoService rabbitDemoService;
    private final UserSessionHelper sessionHelper;

    private static class CompareSession {
        final List<Map<String, String>> kafkaSent = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger kafkaSentCount = new AtomicInteger(0);
    }

    private final ConcurrentHashMap<String, CompareSession> sessions = new ConcurrentHashMap<>();

    private CompareSession session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new CompareSession());
    }

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "compare");
        return "compare";
    }

    @PostMapping("/send")
    @ResponseBody
    public Map<String, Object> send(@RequestBody Map<String, String> request) {
        String sid = sessionHelper.currentSid();
        CompareSession s = session(sid);
        Map<String, Object> response = new HashMap<>();
        String message = request.getOrDefault("message", "test-" + System.currentTimeMillis());
        String time = LocalDateTime.now().format(FMT);

        try {
            kafkaTemplate.send(TOPIC_PREFIX + sid, "compare-key", message);
            kafkaTemplate.flush();
            s.kafkaSentCount.incrementAndGet();
            s.kafkaSent.add(0, Map.of("time", time, "value", message));
            while (s.kafkaSent.size() > 50) s.kafkaSent.remove(s.kafkaSent.size() - 1);

            rabbitDemoService.send(sid, message);

            response.put("success", true);
            response.put("message", message);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    @GetMapping("/status")
    @ResponseBody
    public Map<String, Object> status() {
        String sid = sessionHelper.currentSid();
        CompareSession s = session(sid);
        Map<String, Object> data = new HashMap<>();
        data.put("kafkaSentCount", s.kafkaSentCount.get());
        data.put("kafkaSent", s.kafkaSent);
        data.put("kafkaNote", "Сообщения остаются в topic и могут быть перечитаны");
        data.put("rabbitSentCount", rabbitDemoService.getSentCount(sid));
        data.put("rabbitReceivedCount", rabbitDemoService.getReceivedCount(sid));
        data.put("rabbitSent", rabbitDemoService.getSentMessages(sid));
        data.put("rabbitReceived", rabbitDemoService.getReceivedMessages(sid));
        data.put("rabbitNote", "Сообщения удалены из очереди после прочтения consumer-ом");
        return data;
    }

    @PostMapping("/clear")
    @ResponseBody
    public Map<String, Object> clear() {
        String sid = sessionHelper.currentSid();
        sessions.remove(sid);
        rabbitDemoService.clear(sid);
        return Map.of("success", true);
    }
}
