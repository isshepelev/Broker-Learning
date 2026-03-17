package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Controller
@RequestMapping("/partitions")
@Slf4j
@RequiredArgsConstructor
public class PartitionVisualizationController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdminService adminService;
    private final UserSessionHelper sessionHelper;

    @GetMapping("")
    public String page(Model model) {
        try {
            Set<String> topics = adminService.listTopics();
            topics.retainAll(sessionHelper.currentUserTopics());
            model.addAttribute("topics", topics);
        } catch (Exception e) {
            model.addAttribute("topics", Collections.emptySet());
        }
        model.addAttribute("currentPage", "partitions");
        return "partitions";
    }

    @PostMapping("/send")
    @ResponseBody
    public Map<String, Object> sendAndVisualize(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String topic = (String) request.get("topic");

            // Проверяем ownership
            Set<String> userTopics = sessionHelper.currentUserTopics();
            if (topic == null || !userTopics.contains(topic)) {
                response.put("success", false);
                response.put("error", "Топик не принадлежит вам");
                return response;
            }

            @SuppressWarnings("unchecked")
            List<String> keys = (List<String>) request.get("keys");
            int messagesPerKey = request.get("messagesPerKey") != null
                    ? ((Number) request.get("messagesPerKey")).intValue() : 1;

            List<Map<String, Object>> results = new ArrayList<>();

            for (String key : keys) {
                for (int i = 0; i < messagesPerKey; i++) {
                    String value = "Message from key=" + key + " #" + (i + 1);
                    SendResult<String, String> result = kafkaTemplate.send(topic, key, value)
                            .get(10, TimeUnit.SECONDS);
                    RecordMetadata meta = result.getRecordMetadata();

                    Map<String, Object> entry = new HashMap<>();
                    entry.put("key", key);
                    entry.put("value", value);
                    entry.put("partition", meta.partition());
                    entry.put("offset", meta.offset());
                    results.add(entry);
                }
            }

            // Группируем по partitions
            Map<Integer, List<Map<String, Object>>> byPartition = new TreeMap<>();
            for (Map<String, Object> r : results) {
                int p = (int) r.get("partition");
                byPartition.computeIfAbsent(p, k -> new ArrayList<>()).add(r);
            }

            // Общее кол-во partitions в topic
            int totalPartitions = 0;
            try {
                var info = adminService.getTopicInfo(topic);
                if (info != null) totalPartitions = info.getPartitions();
            } catch (Exception ignored) {}

            response.put("success", true);
            response.put("messages", results);
            response.put("byPartition", byPartition);
            response.put("totalPartitions", totalPartitions);
            response.put("totalMessages", results.size());
        } catch (Exception e) {
            log.error("Partition demo failed: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }
}
