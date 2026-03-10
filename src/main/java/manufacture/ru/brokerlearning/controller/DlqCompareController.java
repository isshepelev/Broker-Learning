package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.service.DlqCompareService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/dlq-compare")
@RequiredArgsConstructor
public class DlqCompareController {

    private final DlqCompareService dlqCompareService;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "dlq-compare");
        return "dlq-compare";
    }

    @PostMapping("/send")
    @ResponseBody
    public Map<String, Object> send(@RequestBody Map<String, String> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String message = request.getOrDefault("message", "test");
            dlqCompareService.sendToBoth(message);
            response.put("success", true);
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
        data.put("kafkaEvents", dlqCompareService.getKafkaEvents());
        data.put("rabbitEvents", dlqCompareService.getRabbitEvents());
        data.put("kafkaDead", dlqCompareService.getKafkaDead());
        data.put("rabbitDead", dlqCompareService.getRabbitDead());
        return data;
    }

    @PostMapping("/clear")
    @ResponseBody
    public Map<String, Object> clear() {
        dlqCompareService.clear();
        return Map.of("success", true);
    }
}
