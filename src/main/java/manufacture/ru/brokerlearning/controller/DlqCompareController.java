package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.service.DlqCompareService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/dlq-compare")
@RequiredArgsConstructor
public class DlqCompareController {

    private final DlqCompareService dlqCompareService;
    private final UserSessionHelper sessionHelper;

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
            dlqCompareService.sendToBoth(sessionHelper.currentSid(), request.getOrDefault("message", "test"));
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
        String sid = sessionHelper.currentSid();
        Map<String, Object> data = new HashMap<>();
        data.put("kafkaEvents", dlqCompareService.getKafkaEvents(sid));
        data.put("rabbitEvents", dlqCompareService.getRabbitEvents(sid));
        data.put("kafkaDead", dlqCompareService.getKafkaDead(sid));
        data.put("rabbitDead", dlqCompareService.getRabbitDead(sid));
        return data;
    }

    @PostMapping("/clear")
    @ResponseBody
    public Map<String, Object> clear() {
        dlqCompareService.clear(sessionHelper.currentSid());
        return Map.of("success", true);
    }

    @PostMapping("/retry")
    @ResponseBody
    public Map<String, Object> retry(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        String source = (String) request.getOrDefault("source", "");
        int index = request.containsKey("index") ? ((Number) request.get("index")).intValue() : -1;
        if ("kafka".equals(source)) return dlqCompareService.retryKafkaDead(sid, index);
        if ("rabbit".equals(source)) return dlqCompareService.retryRabbitDead(sid, index);
        return Map.of("error", "Unknown source");
    }

    @PostMapping("/retry-all")
    @ResponseBody
    public Map<String, Object> retryAll(@RequestBody Map<String, String> request) {
        String sid = sessionHelper.currentSid();
        String source = request.getOrDefault("source", "");
        if ("kafka".equals(source)) return dlqCompareService.retryAllKafkaDead(sid);
        if ("rabbit".equals(source)) return dlqCompareService.retryAllRabbitDead(sid);
        return Map.of("error", "Unknown source");
    }

    @PostMapping("/save-to-db")
    @ResponseBody
    public Map<String, Object> saveToDb(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        String source = (String) request.getOrDefault("source", "");
        int index = request.containsKey("index") ? ((Number) request.get("index")).intValue() : -1;
        return dlqCompareService.saveDeadToDb(sid, source, index);
    }

    @PostMapping("/save-all-to-db")
    @ResponseBody
    public Map<String, Object> saveAllToDb(@RequestBody Map<String, String> request) {
        String sid = sessionHelper.currentSid();
        return dlqCompareService.saveAllDeadToDb(sid, request.getOrDefault("source", ""));
    }

    @PostMapping("/delete-dead")
    @ResponseBody
    public Map<String, Object> deleteDead(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        String source = (String) request.getOrDefault("source", "");
        int index = request.containsKey("index") ? ((Number) request.get("index")).intValue() : -1;
        return dlqCompareService.deleteDeadMessage(sid, source, index);
    }

    @GetMapping("/saved")
    @ResponseBody
    public List<Map<String, Object>> savedMessages() {
        return dlqCompareService.getSavedDeadMessages();
    }

    @PostMapping("/delete-saved")
    @ResponseBody
    public Map<String, Object> deleteSaved(@RequestBody Map<String, Object> request) {
        Long id = request.containsKey("id") ? ((Number) request.get("id")).longValue() : null;
        if (id == null) return Map.of("error", "No id");
        return dlqCompareService.deleteSavedMessage(id);
    }

    @PostMapping("/retry-saved")
    @ResponseBody
    public Map<String, Object> retrySaved(@RequestBody Map<String, Object> request) {
        Long id = request.containsKey("id") ? ((Number) request.get("id")).longValue() : null;
        if (id == null) return Map.of("error", "No id");
        return dlqCompareService.retrySavedMessage(sessionHelper.currentSid(), id);
    }
}
