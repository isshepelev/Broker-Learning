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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

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

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

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

    /** Возвращает количество партиций для указанного топика */
    @GetMapping("/topic-partitions")
    @ResponseBody
    public Map<String, Object> getTopicPartitions(@RequestParam String topic) {
        try {
            var info = kafkaAdminService.getTopicInfo(topic);
            return Map.of("topic", topic, "partitions", info.getPartitions());
        } catch (Exception e) {
            return Map.of("error", e.getMessage());
        }
    }

    /** Читает сообщения из конкретного топика, опционально из конкретной партиции */
    @GetMapping("/topic-messages")
    @ResponseBody
    public Map<String, Object> getTopicMessages(@RequestParam String topic,
                                                @RequestParam(required = false) Integer partition,
                                                @RequestParam(defaultValue = "200") int limit) {
        // Проверяем ownership
        String sid = sessionHelper.currentSid();
        Set<String> userTopics = resourceRepository.topicNamesForUser(sid);
        if (!userTopics.contains(topic)) {
            return Map.of("success", false, "error", "Топик не принадлежит вам");
        }

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> messages = new ArrayList<>();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "_browse-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(Math.min(limit, 500)));

        DateTimeFormatter timeFmt = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions;
            if (partition != null) {
                partitions = List.of(new TopicPartition(topic, partition));
            } else {
                partitions = consumer.partitionsFor(topic).stream()
                        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                        .toList();
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            int collected = 0;
            int emptyPolls = 0;
            while (collected < limit && emptyPolls < 2) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) { emptyPolls++; continue; }
                for (ConsumerRecord<String, String> record : records) {
                    if (collected >= limit) break;
                    Map<String, Object> msg = new LinkedHashMap<>();
                    msg.put("offset", record.offset());
                    msg.put("partition", record.partition());
                    msg.put("key", record.key() != null ? record.key() : "");
                    msg.put("value", record.value() != null ? record.value() : "");
                    msg.put("timestamp", timeFmt.format(Instant.ofEpochMilli(record.timestamp())));
                    messages.add(msg);
                    collected++;
                }
            }
            result.put("success", true);
            result.put("messages", messages);
            result.put("count", messages.size());
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
            result.put("messages", List.of());
            result.put("count", 0);
        }
        return result;
    }
}
