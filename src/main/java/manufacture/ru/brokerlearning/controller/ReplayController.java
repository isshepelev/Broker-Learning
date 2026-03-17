package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.model.UserResource;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import manufacture.ru.brokerlearning.service.GroupConsumerDemoService;
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
    private final GroupConsumerDemoService groupDemoService;
    private final UserSessionHelper sessionHelper;
    private final UserResourceRepository resourceRepository;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "replay");
        return "replay";
    }

    @PostMapping("/send")
    @ResponseBody
    public Map<String, Object> send(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        Map<String, Object> response = new HashMap<>();
        try {
            String customMessage = (String) request.get("customMessage");
            if (customMessage != null && !customMessage.isBlank()) {
                Map<String, String> sent = replayService.sendCustomMessage(sid, customMessage);
                response.put("success", true);
                response.put("sent", List.of(sent));
                response.put("count", 1);
            } else {
                int count = request.containsKey("count") ? ((Number) request.get("count")).intValue() : 5;
                List<Map<String, String>> sent = replayService.sendMessages(sid, count);
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
        String sid = sessionHelper.currentSid();
        Map<String, Object> response = new HashMap<>();
        try {
            Map<String, Object> result = replayService.readMessages(sid);
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
        String sid = sessionHelper.currentSid();
        Map<String, Object> response = new HashMap<>();
        try {
            if (request != null && request.containsKey("offset")) {
                long offset = ((Number) request.get("offset")).longValue();
                replayService.resetToOffset(sid, offset);
                response.put("success", true);
                response.put("resetTo", offset);
            } else {
                Map<String, Object> result = replayService.resetToBeginning(sid);
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
        String sid = sessionHelper.currentSid();
        Map<String, Object> response = new HashMap<>();
        try {
            replayService.clearTopic(sid);
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
        return replayService.getStatus(sessionHelper.currentSid());
    }

    // ===== Practice mode =====

    @GetMapping("/practice/topics")
    @ResponseBody
    public Set<String> practiceTopics() {
        Set<String> topics = practiceService.listTopics();
        topics.retainAll(sessionHelper.currentUserTopics());
        return topics;
    }

    @GetMapping("/practice/groups")
    @ResponseBody
    public List<String> practiceGroups() {
        return practiceService.listConsumerGroups();
    }

    @PostMapping("/practice/create-topic")
    @ResponseBody
    public Map<String, Object> practiceCreateTopic(@RequestBody Map<String, Object> request) {
        String topicName = (String) request.getOrDefault("topic", "");
        Map<String, Object> result = practiceService.createTopic(topicName);
        if (result.containsKey("success") && Boolean.TRUE.equals(result.get("success"))) {
            resourceRepository.save(UserResource.builder()
                    .resourceType("TOPIC")
                    .resourceName(topicName.trim())
                    .ownerSid(sessionHelper.currentSid())
                    .build());
        }
        return result;
    }

    @PostMapping("/practice/session")
    @ResponseBody
    public Map<String, Object> practiceStartSession(@RequestBody Map<String, Object> request) {
        return practiceService.startSession(sessionHelper.currentSid(), (String) request.getOrDefault("topic", ""));
    }

    @PostMapping("/practice/end")
    @ResponseBody
    public Map<String, Object> practiceEndSession() {
        return practiceService.endSession(sessionHelper.currentSid());
    }

    @PostMapping("/practice/add-consumer")
    @ResponseBody
    public Map<String, Object> practiceAddConsumer(@RequestBody Map<String, Object> request) {
        return practiceService.addConsumer(sessionHelper.currentSid(), (String) request.getOrDefault("group", ""));
    }

    @PostMapping("/practice/remove-consumer")
    @ResponseBody
    public Map<String, Object> practiceRemoveConsumer(@RequestBody Map<String, Object> request) {
        return practiceService.removeConsumer(sessionHelper.currentSid(), (String) request.getOrDefault("group", ""));
    }

    @PostMapping("/practice/send")
    @ResponseBody
    public Map<String, Object> practiceSend(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        String customMessage = (String) request.get("customMessage");
        if (customMessage != null && !customMessage.isBlank()) {
            return practiceService.sendCustomMessage(sid, customMessage);
        }
        int count = request.containsKey("count") ? ((Number) request.get("count")).intValue() : 5;
        return practiceService.sendMessages(sid, count);
    }

    @PostMapping("/practice/read")
    @ResponseBody
    public Map<String, Object> practiceRead(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        return practiceService.readMessages(sid, (String) request.getOrDefault("group", ""), (String) request.getOrDefault("mode", "all"));
    }

    @PostMapping("/practice/reset")
    @ResponseBody
    public Map<String, Object> practiceReset(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        String group = (String) request.getOrDefault("group", "");
        if (request.containsKey("offset")) {
            return practiceService.resetToOffset(sid, group, ((Number) request.get("offset")).longValue());
        }
        return practiceService.resetToBeginning(sid, group);
    }

    @GetMapping("/practice/status")
    @ResponseBody
    public Map<String, Object> practiceStatus() {
        return practiceService.getStatus(sessionHelper.currentSid());
    }

    // ===== Group consumer demo =====

    @PostMapping("/group-demo/start")
    @ResponseBody
    public Map<String, Object> groupDemoStart(@RequestBody(required = false) Map<String, Object> request) {
        String topic = (request != null) ? (String) request.get("topic") : null;
        return groupDemoService.startSession(sessionHelper.currentSid(), topic);
    }

    @PostMapping("/group-demo/end")
    @ResponseBody
    public Map<String, Object> groupDemoEnd() {
        return groupDemoService.endSession(sessionHelper.currentSid());
    }

    @PostMapping("/group-demo/add-consumer")
    @ResponseBody
    public Map<String, Object> groupDemoAddConsumer() {
        return groupDemoService.addConsumer(sessionHelper.currentSid());
    }

    @PostMapping("/group-demo/remove-consumer")
    @ResponseBody
    public Map<String, Object> groupDemoRemoveConsumer() {
        return groupDemoService.removeConsumer(sessionHelper.currentSid());
    }

    @PostMapping("/group-demo/send")
    @ResponseBody
    public Map<String, Object> groupDemoSend(@RequestBody Map<String, Object> request) {
        String sid = sessionHelper.currentSid();
        String customMessage = (String) request.get("customMessage");
        if (customMessage != null && !customMessage.isBlank()) {
            return groupDemoService.sendCustomMessage(sid, customMessage);
        }
        int count = request.containsKey("count") ? ((Number) request.get("count")).intValue() : 5;
        return groupDemoService.sendMessages(sid, count);
    }

    @GetMapping("/group-demo/status")
    @ResponseBody
    public Map<String, Object> groupDemoStatus() {
        return groupDemoService.getStatus(sessionHelper.currentSid());
    }
}
