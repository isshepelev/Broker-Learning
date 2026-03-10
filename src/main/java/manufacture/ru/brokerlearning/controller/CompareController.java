package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.RabbitDemoService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
@RequestMapping("/compare")
@Slf4j
@RequiredArgsConstructor
public class CompareController {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String KAFKA_TOPIC = "compare-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RabbitDemoService rabbitDemoService;

    // Kafka tracking (in-memory for demo)
    private final List<Map<String, String>> kafkaSent = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger kafkaSentCount = new AtomicInteger(0);

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "compare");
        return "compare";
    }

    /**
     * Отправить одно сообщение в ОБА брокера одновременно.
     */
    @PostMapping("/send")
    @ResponseBody
    public Map<String, Object> send(@RequestBody Map<String, String> request) {
        Map<String, Object> response = new HashMap<>();
        String message = request.getOrDefault("message", "test-" + System.currentTimeMillis());
        String time = LocalDateTime.now().format(FMT);

        try {
            // Kafka
            kafkaTemplate.send(KAFKA_TOPIC, "compare-key", message);
            kafkaTemplate.flush();
            kafkaSentCount.incrementAndGet();
            kafkaSent.add(0, Map.of("time", time, "value", message));
            while (kafkaSent.size() > 50) kafkaSent.remove(kafkaSent.size() - 1);

            // RabbitMQ
            rabbitDemoService.send(message);

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
        Map<String, Object> data = new HashMap<>();

        // Kafka
        data.put("kafkaSentCount", kafkaSentCount.get());
        data.put("kafkaSent", kafkaSent);
        // Kafka messages stay in topic — they can be re-read
        data.put("kafkaNote", "Сообщения остаются в topic и могут быть перечитаны");

        // RabbitMQ
        data.put("rabbitSentCount", rabbitDemoService.getSentCount());
        data.put("rabbitReceivedCount", rabbitDemoService.getReceivedCount());
        data.put("rabbitSent", rabbitDemoService.getSentMessages());
        data.put("rabbitReceived", rabbitDemoService.getReceivedMessages());
        // RabbitMQ messages are removed after consumption
        data.put("rabbitNote", "Сообщения удалены из очереди после прочтения consumer-ом");

        return data;
    }

    @PostMapping("/clear")
    @ResponseBody
    public Map<String, Object> clear() {
        kafkaSent.clear();
        kafkaSentCount.set(0);
        rabbitDemoService.clear();
        return Map.of("success", true);
    }
}
