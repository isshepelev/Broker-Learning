package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.model.UserResource;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Set;

@Controller
@RequestMapping("/topics")
@Slf4j
@RequiredArgsConstructor
public class TopicController {

    private final KafkaAdminService adminService;
    private final UserSessionHelper sessionHelper;
    private final UserResourceRepository resourceRepository;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @GetMapping("")
    public String topicsPage(Model model) {
        try {
            String sid = sessionHelper.currentSid();
            Set<String> userTopics = resourceRepository.topicNamesForUser(sid);
            // Show only topics owned by this user
            Set<String> allTopics = adminService.listTopics();
            allTopics.retainAll(userTopics);
            model.addAttribute("topicNames", allTopics);
        } catch (Exception e) {
            log.warn("Unable to list topics: {}", e.getMessage());
            model.addAttribute("topicNames", Collections.emptySet());
        }
        model.addAttribute("currentPage", "topics");
        return "topics";
    }

    @GetMapping("/{name}")
    public String topicDetail(@PathVariable String name, Model model) {
        // Проверяем ownership
        Set<String> userTopics = resourceRepository.topicNamesForUser(sessionHelper.currentSid());
        if (!userTopics.contains(name)) {
            model.addAttribute("topicInfo", null);
            model.addAttribute("currentPage", "topics");
            return "topic-detail";
        }
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
            // Track ownership
            resourceRepository.save(UserResource.builder()
                    .resourceType("TOPIC")
                    .resourceName(name)
                    .ownerSid(sessionHelper.currentSid())
                    .build());
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
            // Проверяем ownership
            Set<String> userTopics = resourceRepository.topicNamesForUser(sessionHelper.currentSid());
            if (!userTopics.contains(name)) {
                response.put("success", false);
                response.put("error", "Топик не принадлежит вам");
                return ResponseEntity.status(403).body(response);
            }
            adminService.deleteTopic(name);
            resourceRepository.findByResourceTypeAndResourceName("TOPIC", name)
                    .ifPresent(resourceRepository::delete);
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

    @GetMapping("/{name}/messages")
    @ResponseBody
    public Map<String, Object> topicMessages(@PathVariable String name,
                                             @RequestParam(defaultValue = "500") int limit) {
        // Проверяем ownership
        Set<String> userTopics = resourceRepository.topicNamesForUser(sessionHelper.currentSid());
        if (!userTopics.contains(name)) {
            return Map.of("success", false, "error", "Топик не принадлежит вам", "messages", List.of(), "count", 0);
        }

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> messages = new ArrayList<>();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "_topic-detail-reader-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(Math.min(limit, 500)));

        DateTimeFormatter timeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(name).stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .toList();
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
            log.error("Failed to read messages from topic '{}': {}", name, e.getMessage());
            result.put("success", false);
            result.put("error", e.getMessage());
            result.put("messages", List.of());
            result.put("count", 0);
        }
        return result;
    }
}
