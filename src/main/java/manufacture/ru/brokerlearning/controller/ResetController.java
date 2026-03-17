package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.job.JobManagementService;
import manufacture.ru.brokerlearning.model.UserResource;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import manufacture.ru.brokerlearning.service.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/reset")
@Slf4j
@RequiredArgsConstructor
public class ResetController {

    private final AdminClient adminClient;
    private final DynamicConsumerService dynamicConsumerService;
    private final DlqCompareService dlqCompareService;
    private final ReplayPracticeService replayPracticeService;
    private final ReplayService replayService;
    private final RebalancingService rebalancingService;
    private final RebalancingPracticeService rebalancingPracticeService;
    private final OrderingService orderingService;
    private final JobManagementService jobManagementService;
    private final UserSessionHelper sessionHelper;
    private final UserResourceRepository resourceRepository;

    @PostMapping
    public Map<String, Object> resetAll() {
        String sid = sessionHelper.currentSid();
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> actions = new ArrayList<>();

        try {
            dynamicConsumerService.stopAllForUser(sid);
            actions.add("Динамические consumer-ы остановлены");
        } catch (Exception e) {
            actions.add("Ошибка остановки consumer-ов: " + e.getMessage());
        }

        try {
            replayPracticeService.endSession(sid);
            actions.add("Practice-сессия завершена");
        } catch (Exception e) {
            actions.add("Ошибка: " + e.getMessage());
        }

        try {
            replayService.cleanupSession(sid);
            actions.add("Replay-ресурсы пользователя удалены");
        } catch (Exception e) {
            actions.add("Ошибка: " + e.getMessage());
        }

        try {
            dlqCompareService.clear(sid);
            actions.add("DLQ-состояние очищено");
        } catch (Exception e) {
            actions.add("Ошибка: " + e.getMessage());
        }

        try { rebalancingService.cleanupSession(sid); actions.add("Rebalancing demo очищен"); } catch (Exception e) { actions.add("Ошибка: " + e.getMessage()); }
        try { rebalancingPracticeService.cleanupSession(sid); actions.add("Rebalancing practice очищен"); } catch (Exception e) { actions.add("Ошибка: " + e.getMessage()); }
        try { orderingService.cleanupSession(sid); actions.add("Ordering очищен"); } catch (Exception e) { actions.add("Ошибка: " + e.getMessage()); }
        try { jobManagementService.cleanup(sid); actions.add("Jobs остановлены"); } catch (Exception e) { actions.add("Ошибка: " + e.getMessage()); }

        // Delete user-owned Kafka topics and consumer groups
        try {
            List<UserResource> userResources = resourceRepository.findByOwnerSid(sid);
            List<String> topicsToDelete = new ArrayList<>();
            List<String> groupsToDelete = new ArrayList<>();
            for (UserResource r : userResources) {
                if ("TOPIC".equals(r.getResourceType())) topicsToDelete.add(r.getResourceName());
                if ("CONSUMER_GROUP".equals(r.getResourceType())) groupsToDelete.add(r.getResourceName());
            }
            if (!topicsToDelete.isEmpty()) {
                try { adminClient.deleteTopics(topicsToDelete).all().get(); } catch (Exception ignored) {}
                actions.add("Удалено " + topicsToDelete.size() + " топиков: " + String.join(", ", topicsToDelete));
            }
            if (!groupsToDelete.isEmpty()) {
                try { adminClient.deleteConsumerGroups(groupsToDelete).all().get(); } catch (Exception ignored) {}
                actions.add("Удалено " + groupsToDelete.size() + " consumer group");
            }
            resourceRepository.deleteAll(userResources);
        } catch (Exception e) {
            actions.add("Ошибка удаления ресурсов: " + e.getMessage());
        }

        result.put("success", true);
        result.put("actions", actions);
        log.info("Reset completed for user {} (sid={}): {}", sessionHelper.currentUsername(), sid, actions);
        return result;
    }
}
