package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.service.DynamicConsumerService;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("/consumer")
@Slf4j
@RequiredArgsConstructor
public class ConsumerController {

    private final MessageHistoryService historyService;
    private final DynamicConsumerService dynamicConsumerService;
    private final KafkaAdminService kafkaAdminService;

    @GetMapping("")
    public String consumerPage(Model model) {
        model.addAttribute("currentPage", "consumer");
        return "consumer";
    }

    @GetMapping("/messages")
    @ResponseBody
    public List<KafkaMessageEntity> getRecentMessages() {
        return historyService.getMessagesReceivedSinceStart();
    }

    @GetMapping("/topics")
    @ResponseBody
    public Set<String> getTopics() {
        try {
            return kafkaAdminService.listTopics();
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
        return dynamicConsumerService.createConsumer(name, topic, groupId);
    }

    @PostMapping("/stop")
    @ResponseBody
    public Map<String, Object> stopConsumer(@RequestBody Map<String, String> request) {
        String name = request.get("name");
        return dynamicConsumerService.stopConsumer(name);
    }

    @PostMapping("/stop-all")
    @ResponseBody
    public Map<String, Object> stopAll() {
        return dynamicConsumerService.stopAll();
    }

    @GetMapping("/list")
    @ResponseBody
    public List<Map<String, Object>> listConsumers() {
        return dynamicConsumerService.listConsumers();
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
        return dynamicConsumerService.getAllMessages();
    }

    @GetMapping("/partitions")
    @ResponseBody
    public Set<Integer> getConsumerPartitions(@RequestParam String consumerName) {
        return dynamicConsumerService.getPartitions(consumerName);
    }
}
