package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
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
    private final UserSessionHelper sessionHelper;

    @GetMapping("/")
    public String dashboard(Model model) {
        try {
            java.util.Set<String> topics = adminService.listTopics();
            topics.retainAll(sessionHelper.currentUserTopics());
            model.addAttribute("topics", topics);
        } catch (Exception e) {
            log.warn("Kafka unavailable, unable to list topics: {}", e.getMessage());
            model.addAttribute("topics", Collections.emptySet());
        }

        try {
            model.addAttribute("recentMessages", historyService.getRecentMessages(sessionHelper.currentSid()));
        } catch (Exception e) {
            log.warn("Unable to fetch recent messages: {}", e.getMessage());
            model.addAttribute("recentMessages", Collections.emptyList());
        }

        try {
            model.addAttribute("jobStatuses", jobService.getJobStatuses(sessionHelper.currentSid()));
        } catch (Exception e) {
            log.warn("Unable to fetch job statuses: {}", e.getMessage());
            model.addAttribute("jobStatuses", Collections.emptyMap());
        }

        model.addAttribute("currentPage", "dashboard");
        return "dashboard";
    }
}
