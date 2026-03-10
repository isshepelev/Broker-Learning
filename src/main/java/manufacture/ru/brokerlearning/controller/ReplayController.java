package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.ReplayService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/replay")
@Slf4j
@RequiredArgsConstructor
public class ReplayController {

    private final ReplayService replayService;

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
}
