package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import manufacture.ru.brokerlearning.service.RebalancingPracticeService;
import manufacture.ru.brokerlearning.service.RebalancingService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("/rebalancing")
@RequiredArgsConstructor
public class RebalancingController {

    private final RebalancingService rebalancingService;
    private final RebalancingPracticeService practiceService;
    private final UserSessionHelper sessionHelper;
    private final KafkaAdminService adminService;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "rebalancing");
        return "rebalancing";
    }

    // ───── Демо ─────

    @PostMapping("/add")
    @ResponseBody
    public Map<String, Object> addConsumer() {
        return rebalancingService.addConsumer(sessionHelper.currentSid());
    }

    @PostMapping("/remove")
    @ResponseBody
    public Map<String, Object> removeConsumer() {
        return rebalancingService.removeConsumer(sessionHelper.currentSid());
    }

    @PostMapping("/add-partition")
    @ResponseBody
    public Map<String, Object> addPartition() {
        return rebalancingService.addPartition(sessionHelper.currentSid());
    }

    @PostMapping("/reset")
    @ResponseBody
    public Map<String, Object> reset() {
        return rebalancingService.reset(sessionHelper.currentSid());
    }

    @GetMapping("/status")
    @ResponseBody
    public Map<String, Object> status() {
        return rebalancingService.getStatus(sessionHelper.currentSid());
    }

    // ───── Практика ─────

    @GetMapping("/practice/topics")
    @ResponseBody
    public Set<String> practiceTopics() {
        try {
            Set<String> topics = adminService.listTopics();
            topics.retainAll(sessionHelper.currentUserTopics());
            return topics;
        } catch (Exception e) {
            return Set.of();
        }
    }

    @PostMapping("/practice/create-session")
    @ResponseBody
    public Map<String, Object> practiceCreateSession(
            @RequestParam String topicName,
            @RequestParam String groupId,
            @RequestParam int partitions) {
        return practiceService.createSession(sessionHelper.currentSid(), topicName, groupId, partitions);
    }

    @PostMapping("/practice/add")
    @ResponseBody
    public Map<String, Object> practiceAddConsumer() {
        return practiceService.addConsumer(sessionHelper.currentSid());
    }

    @PostMapping("/practice/remove")
    @ResponseBody
    public Map<String, Object> practiceRemoveConsumer() {
        return practiceService.removeConsumer(sessionHelper.currentSid());
    }

    @PostMapping("/practice/add-partition")
    @ResponseBody
    public Map<String, Object> practiceAddPartition() {
        return practiceService.addPartition(sessionHelper.currentSid());
    }

    @PostMapping("/practice/reset")
    @ResponseBody
    public Map<String, Object> practiceReset() {
        return practiceService.reset(sessionHelper.currentSid());
    }

    @GetMapping("/practice/status")
    @ResponseBody
    public Map<String, Object> practiceStatus() {
        return practiceService.getStatus(sessionHelper.currentSid());
    }
}
