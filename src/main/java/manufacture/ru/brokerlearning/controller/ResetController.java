package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.InternalKafkaRegistry;
import manufacture.ru.brokerlearning.repository.KafkaMessageRepository;
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
    private final KafkaMessageRepository messageRepository;

    @PostMapping
    public Map<String, Object> resetAll() {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> actions = new ArrayList<>();

        // 1. Останавливаем динамических consumer-ов
        try {
            dynamicConsumerService.stopAll();
            actions.add("Динамические consumer-ы остановлены");
        } catch (Exception e) {
            actions.add("Ошибка остановки consumer-ов: " + e.getMessage());
        }

        // 2. Завершаем practice-сессию
        try {
            replayPracticeService.endSession();
            actions.add("Practice-сессия завершена");
        } catch (Exception e) {
            actions.add("Ошибка завершения practice-сессии: " + e.getMessage());
        }

        // 3. Очищаем replay-topic
        try {
            replayService.clearTopic();
            actions.add("replay-topic очищен");
        } catch (Exception e) {
            actions.add("Ошибка очистки replay-topic: " + e.getMessage());
        }

        // 4. Очищаем DLQ-состояние
        try {
            dlqCompareService.clear();
            actions.add("DLQ-состояние очищено");
        } catch (Exception e) {
            actions.add("Ошибка очистки DLQ: " + e.getMessage());
        }

        // 5. Удаляем пользовательские топики
        try {
            Set<String> allTopics = adminClient.listTopics().names().get();
            List<String> toDelete = new ArrayList<>();
            for (String topic : allTopics) {
                if (InternalKafkaRegistry.isUserTopic(topic)) {
                    toDelete.add(topic);
                }
            }
            if (!toDelete.isEmpty()) {
                adminClient.deleteTopics(toDelete).all().get();
                actions.add("Удалено " + toDelete.size() + " пользовательских топиков: " + String.join(", ", toDelete));
            } else {
                actions.add("Пользовательских топиков не найдено");
            }
        } catch (Exception e) {
            actions.add("Ошибка удаления топиков: " + e.getMessage());
        }

        // 6. Удаляем пользовательские consumer group-ы
        try {
            List<String> allGroups = adminClient.listConsumerGroups().all().get().stream()
                    .map(g -> g.groupId())
                    .filter(InternalKafkaRegistry::isUserGroup)
                    .toList();
            if (!allGroups.isEmpty()) {
                adminClient.deleteConsumerGroups(allGroups).all().get();
                actions.add("Удалено " + allGroups.size() + " пользовательских consumer group");
            } else {
                actions.add("Пользовательских consumer group не найдено");
            }
        } catch (Exception e) {
            actions.add("Ошибка удаления consumer group: " + e.getMessage());
        }

        // 7. Очищаем БД
        try {
            long count = messageRepository.count();
            messageRepository.deleteAll();
            actions.add("Удалено " + count + " записей из БД");
        } catch (Exception e) {
            actions.add("Ошибка очистки БД: " + e.getMessage());
        }

        result.put("success", true);
        result.put("actions", actions);
        log.info("Full reset completed: {}", actions);
        return result;
    }
}
