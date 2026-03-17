package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.job.JobManagementService;
import manufacture.ru.brokerlearning.model.AppUser;
import manufacture.ru.brokerlearning.model.UserResource;
import manufacture.ru.brokerlearning.repository.AppUserRepository;
import manufacture.ru.brokerlearning.repository.KafkaMessageRepository;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import manufacture.ru.brokerlearning.service.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Controller
@RequestMapping("/admin")
@Slf4j
@RequiredArgsConstructor
public class AdminController {

    private final AppUserRepository userRepository;
    private final UserResourceRepository resourceRepository;
    private final KafkaMessageRepository messageRepository;
    private final AdminClient adminClient;
    private final ReplayService replayService;
    private final ReplayPracticeService replayPracticeService;
    private final DlqCompareService dlqCompareService;
    private final RebalancingService rebalancingService;
    private final RebalancingPracticeService rebalancingPracticeService;
    private final OrderingService orderingService;
    private final JobManagementService jobManagementService;
    private final DynamicConsumerService dynamicConsumerService;

    @GetMapping("")
    public String adminPage(Model model) {
        List<AppUser> users = userRepository.findAll();
        List<Map<String, Object>> userInfos = new ArrayList<>();
        for (AppUser user : users) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("id", user.getId());
            info.put("username", user.getUsername());
            info.put("sid", user.getSid());
            info.put("role", user.getRole());
            info.put("createdAt", user.getCreatedAt());
            List<UserResource> resources = resourceRepository.findByOwnerSid(user.getSid());
            info.put("topicCount", resources.stream().filter(r -> "TOPIC".equals(r.getResourceType())).count());
            info.put("groupCount", resources.stream().filter(r -> "CONSUMER_GROUP".equals(r.getResourceType())).count());
            userInfos.add(info);
        }
        model.addAttribute("users", userInfos);
        model.addAttribute("currentPage", "admin");
        return "admin";
    }

    /** Reset конкретного пользователя — очищает все его ресурсы */
    @PostMapping("/reset-user/{userId}")
    @ResponseBody
    public Map<String, Object> resetUser(@PathVariable Long userId) {
        Optional<AppUser> opt = userRepository.findById(userId);
        if (opt.isEmpty()) return Map.of("error", "Пользователь не найден");
        AppUser user = opt.get();
        String sid = user.getSid();

        List<String> actions = new ArrayList<>();
        cleanupUserResources(sid, actions);

        log.info("Admin reset user {} (sid={}): {}", user.getUsername(), sid, actions);
        return Map.of("success", true, "actions", actions);
    }

    /** Удалить пользователя полностью — все данные + аккаунт */
    @PostMapping("/delete-user/{userId}")
    @ResponseBody
    public Map<String, Object> deleteUser(@PathVariable Long userId) {
        Optional<AppUser> opt = userRepository.findById(userId);
        if (opt.isEmpty()) return Map.of("error", "Пользователь не найден");
        AppUser user = opt.get();

        if ("ROLE_ADMIN".equals(user.getRole())) {
            return Map.of("error", "Нельзя удалить администратора");
        }

        String sid = user.getSid();
        List<String> actions = new ArrayList<>();

        cleanupUserResources(sid, actions);

        // Удаляем сообщения из БД
        try {
            List<manufacture.ru.brokerlearning.model.KafkaMessageEntity> msgs =
                    messageRepository.findTop100ByOwnerSidOrderByTimestampDesc(sid);
            // Удаляем все сообщения этого пользователя
            messageRepository.deleteAll(msgs);
            actions.add("Удалено сообщений из БД: " + msgs.size());
        } catch (Exception e) {
            actions.add("Ошибка удаления сообщений: " + e.getMessage());
        }

        // Удаляем аккаунт
        userRepository.delete(user);
        actions.add("Аккаунт '" + user.getUsername() + "' удалён");

        log.info("Admin deleted user {} (sid={}): {}", user.getUsername(), sid, actions);
        return Map.of("success", true, "actions", actions);
    }

    private void cleanupUserResources(String sid, List<String> actions) {
        // Останавливаем in-memory ресурсы
        try { dynamicConsumerService.stopAllForUser(sid); actions.add("Dynamic consumers остановлены"); } catch (Exception e) { actions.add("Ошибка: " + e.getMessage()); }
        try { replayService.cleanupSession(sid); actions.add("Replay очищен"); } catch (Exception e) { actions.add("Ошибка: " + e.getMessage()); }
        try { replayPracticeService.cleanupSession(sid); } catch (Exception ignored) {}
        try { dlqCompareService.cleanupSession(sid); } catch (Exception ignored) {}
        try { rebalancingService.cleanupSession(sid); } catch (Exception ignored) {}
        try { rebalancingPracticeService.cleanupSession(sid); } catch (Exception ignored) {}
        try { orderingService.cleanupSession(sid); } catch (Exception ignored) {}
        try { jobManagementService.cleanup(sid); } catch (Exception ignored) {}

        // Удаляем Kafka-ресурсы
        List<UserResource> resources = resourceRepository.findByOwnerSid(sid);
        List<String> topicsToDelete = new ArrayList<>();
        List<String> groupsToDelete = new ArrayList<>();
        for (UserResource r : resources) {
            if ("TOPIC".equals(r.getResourceType())) topicsToDelete.add(r.getResourceName());
            if ("CONSUMER_GROUP".equals(r.getResourceType())) groupsToDelete.add(r.getResourceName());
        }
        if (!topicsToDelete.isEmpty()) {
            try { adminClient.deleteTopics(topicsToDelete).all().get(); } catch (Exception ignored) {}
            actions.add("Удалено топиков: " + topicsToDelete.size());
        }
        if (!groupsToDelete.isEmpty()) {
            try { adminClient.deleteConsumerGroups(groupsToDelete).all().get(); } catch (Exception ignored) {}
            actions.add("Удалено consumer group: " + groupsToDelete.size());
        }
        resourceRepository.deleteAll(resources);
    }
}
