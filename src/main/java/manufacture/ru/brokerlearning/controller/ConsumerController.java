package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.model.UserResource;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import manufacture.ru.brokerlearning.service.DynamicConsumerService;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Controller
@RequestMapping("/consumer")
@Slf4j
@RequiredArgsConstructor
public class ConsumerController {

    private final MessageHistoryService historyService;
    private final DynamicConsumerService dynamicConsumerService;
    private final KafkaAdminService kafkaAdminService;
    private final UserSessionHelper sessionHelper;
    private final UserResourceRepository resourceRepository;

    @GetMapping("")
    public String consumerPage(Model model) {
        model.addAttribute("currentPage", "consumer");
        return "consumer";
    }

    @GetMapping("/messages")
    @ResponseBody
    public List<KafkaMessageEntity> getRecentMessages() {
        return historyService.getMessagesReceivedSinceStart(sessionHelper.currentSid());
    }

    @GetMapping("/topics")
    @ResponseBody
    public Set<String> getTopics() {
        try {
            String sid = sessionHelper.currentSid();
            Set<String> userTopics = resourceRepository.topicNamesForUser(sid);
            Set<String> allTopics = kafkaAdminService.listTopics();
            allTopics.retainAll(userTopics);
            return allTopics;
        } catch (Exception e) {
            log.error("Failed to list topics", e);
            return Set.of();
        }
    }

    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createConsumer(@RequestBody Map<String, String> request) {
        String name = request.get("name");
        String topic = request.get("topic");
        String groupId = request.get("groupId");
        if (topic == null || topic.isBlank()) {
            return Map.of("success", false, "error", "Topic обязателен");
        }
        String sid = sessionHelper.currentSid();

        // Проверяем что топик принадлежит текущему пользователю
        Set<String> userTopics = resourceRepository.topicNamesForUser(sid);
        if (!userTopics.contains(topic)) {
            return Map.of("success", false, "error", "Топик '" + topic + "' не принадлежит вам");
        }

        // Проверяем что consumer group не занята другим пользователем
        String resolvedGroup = (groupId != null && !groupId.isBlank()) ? groupId : "dynamic-" + (name != null && !name.isBlank() ? name : "consumer");
        Optional<UserResource> existingGroup = resourceRepository.findByResourceTypeAndResourceName("CONSUMER_GROUP", resolvedGroup);
        if (existingGroup.isPresent() && !existingGroup.get().getOwnerSid().equals(sid)) {
            return Map.of("success", false, "error", "Consumer group '" + resolvedGroup + "' уже используется другим пользователем");
        }

        Map<String, Object> result = dynamicConsumerService.createConsumer(name, topic, groupId, sid);
        if (Boolean.TRUE.equals(result.get("success"))) {
            // Регистрируем ownership группы если ещё не зарегистрирована
            if (existingGroup.isEmpty()) {
                resourceRepository.save(UserResource.builder()
                        .resourceType("CONSUMER_GROUP")
                        .resourceName(resolvedGroup)
                        .ownerSid(sid)
                        .build());
            }
        }
        return result;
    }

    @PostMapping("/stop")
    @ResponseBody
    public Map<String, Object> stopConsumer(@RequestBody Map<String, String> request) {
        return dynamicConsumerService.stopConsumer(request.get("name"));
    }

    @PostMapping("/stop-all")
    @ResponseBody
    public Map<String, Object> stopAll() {
        return dynamicConsumerService.stopAllForUser(sessionHelper.currentSid());
    }

    @GetMapping("/list")
    @ResponseBody
    public List<Map<String, Object>> listConsumers() {
        return dynamicConsumerService.listConsumers(sessionHelper.currentSid());
    }

    @GetMapping("/dynamic-messages")
    @ResponseBody
    public List<Map<String, Object>> getDynamicMessages(
            @RequestParam String consumerName,
            @RequestParam(required = false) Integer partition) {
        return dynamicConsumerService.getMessages(consumerName, partition);
    }

    @GetMapping("/dynamic-messages-all")
    @ResponseBody
    public List<Map<String, Object>> getAllDynamicMessages() {
        return dynamicConsumerService.getAllMessages(sessionHelper.currentSid());
    }

    @GetMapping("/partitions")
    @ResponseBody
    public Set<Integer> getConsumerPartitions(@RequestParam String consumerName) {
        return dynamicConsumerService.getPartitions(consumerName);
    }
}
