package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.job.JobManagementService;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Collections;

@Controller
@Slf4j
@RequiredArgsConstructor
public class DashboardController {

    private final KafkaAdminService adminService;
    private final MessageHistoryService historyService;
    private final JobManagementService jobService;

    @GetMapping("/")
    public String dashboard(Model model) {
        try {
            model.addAttribute("topics", adminService.listTopics());
        } catch (Exception e) {
            log.warn("Kafka unavailable, unable to list topics: {}", e.getMessage());
            model.addAttribute("topics", Collections.emptySet());
        }

        try {
            model.addAttribute("recentMessages", historyService.getRecentMessages());
        } catch (Exception e) {
            log.warn("Unable to fetch recent messages: {}", e.getMessage());
            model.addAttribute("recentMessages", Collections.emptyList());
        }

        try {
            model.addAttribute("jobStatuses", jobService.getJobStatuses());
        } catch (Exception e) {
            log.warn("Unable to fetch job statuses: {}", e.getMessage());
            model.addAttribute("jobStatuses", Collections.emptyMap());
        }

        model.addAttribute("currentPage", "dashboard");
        return "dashboard";
    }
}
