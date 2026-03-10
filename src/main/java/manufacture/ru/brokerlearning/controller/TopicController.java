package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/topics")
@Slf4j
@RequiredArgsConstructor
public class TopicController {

    private final KafkaAdminService adminService;

    @GetMapping("")
    public String topicsPage(Model model) {
        try {
            model.addAttribute("topicNames", adminService.listTopics());
        } catch (Exception e) {
            log.warn("Unable to list topics: {}", e.getMessage());
            model.addAttribute("topicNames", Collections.emptySet());
        }
        model.addAttribute("currentPage", "topics");
        return "topics";
    }

    @GetMapping("/{name}")
    public String topicDetail(@PathVariable String name, Model model) {
        try {
            model.addAttribute("topicInfo", adminService.getTopicInfo(name));
            model.addAttribute("configs", adminService.getTopicConfigs(name));
        } catch (Exception e) {
            log.error("Unable to get topic details for '{}': {}", name, e.getMessage());
            model.addAttribute("topicInfo", null);
            model.addAttribute("configs", Collections.emptyMap());
        }
        model.addAttribute("currentPage", "topics");
        return "topic-detail";
    }

    @PostMapping("/create")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> createTopic(
            @RequestParam String name,
            @RequestParam(defaultValue = "1") int partitions,
            @RequestParam(defaultValue = "1") short replicationFactor) {
        Map<String, Object> response = new HashMap<>();
        try {
            adminService.createTopic(name, partitions, replicationFactor);
            response.put("success", true);
            response.put("message", "Topic '" + name + "' created successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to create topic '{}': {}", name, e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to create topic: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @DeleteMapping("/delete/{name}")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deleteTopic(@PathVariable String name) {
        Map<String, Object> response = new HashMap<>();
        try {
            adminService.deleteTopic(name);
            response.put("success", true);
            response.put("message", "Topic '" + name + "' deleted successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to delete topic '{}': {}", name, e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to delete topic: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
