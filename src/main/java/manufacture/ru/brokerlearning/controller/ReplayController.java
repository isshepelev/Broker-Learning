package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.ReplayPracticeService;
import manufacture.ru.brokerlearning.service.ReplayService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("/replay")
@Slf4j
@RequiredArgsConstructor
public class ReplayController {

    private final ReplayService replayService;
    private final ReplayPracticeService practiceService;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "replay");
        return "replay";
    }

    @PostMapping("/send")
    @ResponseBody
    public Map<String, Object> send(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String customMessage = (String) request.get("customMessage");
            if (customMessage != null && !customMessage.isBlank()) {
                Map<String, String> sent = replayService.sendCustomMessage(customMessage);
                response.put("success", true);
                response.put("sent", List.of(sent));
                response.put("count", 1);
            } else {
                int count = request.containsKey("count") ? ((Number) request.get("count")).intValue() : 5;
                List<Map<String, String>> sent = replayService.sendMessages(count);
                response.put("success", true);
                response.put("sent", sent);
                response.put("count", sent.size());
            }
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    @PostMapping("/read")
    @ResponseBody
    public Map<String, Object> read() {
        Map<String, Object> response = new HashMap<>();
        try {
            Map<String, Object> result = replayService.readMessages();
            response.put("success", true);
            response.putAll(result);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    @PostMapping("/reset")
    @ResponseBody
    public Map<String, Object> reset(@RequestBody(required = false) Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            if (request != null && request.containsKey("offset")) {
                long offset = ((Number) request.get("offset")).longValue();
                replayService.resetToOffset(offset);
                response.put("success", true);
                response.put("resetTo", offset);
            } else {
                Map<String, Object> result = replayService.resetToBeginning();
                response.putAll(result);
            }
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    @PostMapping("/clear")
    @ResponseBody
    public Map<String, Object> clear() {
        Map<String, Object> response = new HashMap<>();
        try {
            replayService.clearTopic();
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
        return replayService.getStatus();
    }

    // ===== Practice mode endpoints =====

    @GetMapping("/practice/topics")
    @ResponseBody
    public Set<String> practiceTopics() {
        return practiceService.listTopics();
    }

    @GetMapping("/practice/groups")
    @ResponseBody
    public List<String> practiceGroups() {
        return practiceService.listConsumerGroups();
    }

    @PostMapping("/practice/create-topic")
    @ResponseBody
    public Map<String, Object> practiceCreateTopic(@RequestBody Map<String, Object> request) {
        String topic = (String) request.getOrDefault("topic", "");
        return practiceService.createTopic(topic);
    }

    @PostMapping("/practice/session")
    @ResponseBody
    public Map<String, Object> practiceStartSession(@RequestBody Map<String, Object> request) {
        String topic = (String) request.getOrDefault("topic", "");
        return practiceService.startSession(topic);
    }

    @PostMapping("/practice/end")
    @ResponseBody
    public Map<String, Object> practiceEndSession() {
        return practiceService.endSession();
    }

    @PostMapping("/practice/add-consumer")
    @ResponseBody
    public Map<String, Object> practiceAddConsumer(@RequestBody Map<String, Object> request) {
        String group = (String) request.getOrDefault("group", "");
        return practiceService.addConsumer(group);
    }

    @PostMapping("/practice/remove-consumer")
    @ResponseBody
    public Map<String, Object> practiceRemoveConsumer(@RequestBody Map<String, Object> request) {
        String group = (String) request.getOrDefault("group", "");
        return practiceService.removeConsumer(group);
    }

    @PostMapping("/practice/send")
    @ResponseBody
    public Map<String, Object> practiceSend(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String customMessage = (String) request.get("customMessage");
            if (customMessage != null && !customMessage.isBlank()) {
                return practiceService.sendCustomMessage(customMessage);
            } else {
                int count = request.containsKey("count") ? ((Number) request.get("count")).intValue() : 5;
                return practiceService.sendMessages(count);
            }
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    @PostMapping("/practice/read")
    @ResponseBody
    public Map<String, Object> practiceRead(@RequestBody Map<String, Object> request) {
        String group = (String) request.getOrDefault("group", "");
        String mode = (String) request.getOrDefault("mode", "all");
        return practiceService.readMessages(group, mode);
    }

    @PostMapping("/practice/reset")
    @ResponseBody
    public Map<String, Object> practiceReset(@RequestBody Map<String, Object> request) {
        String group = (String) request.getOrDefault("group", "");
        if (request.containsKey("offset")) {
            long offset = ((Number) request.get("offset")).longValue();
            return practiceService.resetToOffset(group, offset);
        }
        return practiceService.resetToBeginning(group);
    }

    @GetMapping("/practice/status")
    @ResponseBody
    public Map<String, Object> practiceStatus() {
        return practiceService.getStatus();
    }
}
