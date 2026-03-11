package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/consumer-groups")
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final AdminClient adminClient;

    @GetMapping("")
    public String consumerGroupsPage(Model model) {
        model.addAttribute("currentPage", "consumer-groups");
        return "consumer-groups";
    }

    @GetMapping("/list")
    @ResponseBody
    public List<Map<String, Object>> listGroups() {
        try {
            var listings = adminClient.listConsumerGroups().all().get();
            var groupIds = listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());

            if (groupIds.isEmpty()) return Collections.emptyList();

            var descriptions = adminClient.describeConsumerGroups(groupIds).all().get();

            List<Map<String, Object>> result = new ArrayList<>();
            for (var entry : descriptions.entrySet()) {
                var desc = entry.getValue();
                Map<String, Object> group = new LinkedHashMap<>();
                group.put("groupId", entry.getKey());
                group.put("state", desc.state().toString());
                group.put("members", desc.members().size());
                group.put("coordinator", desc.coordinator().host() + ":" + desc.coordinator().port());
                group.put("partitionAssignor", desc.partitionAssignor());
                result.add(group);
            }
            result.sort(Comparator.comparing(m -> (String) m.get("groupId")));
            return result;
        } catch (Exception e) {
            log.error("Failed to list consumer groups: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    @GetMapping("/detail/{groupId}")
    @ResponseBody
    public Map<String, Object> groupDetail(@PathVariable String groupId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupId", groupId);

        try {
            // Описание группы
            var desc = adminClient.describeConsumerGroups(List.of(groupId)).all().get().get(groupId);
            result.put("state", desc.state().toString());
            result.put("partitionAssignor", desc.partitionAssignor());
            result.put("coordinator", desc.coordinator().host() + ":" + desc.coordinator().port());

            // Участники и их назначения
            List<Map<String, Object>> members = new ArrayList<>();
            for (var member : desc.members()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("memberId", shorten(member.consumerId()));
                m.put("clientId", member.clientId());
                m.put("host", member.host());

                List<String> partitions = member.assignment().topicPartitions().stream()
                        .map(tp -> tp.topic() + "-" + tp.partition())
                        .sorted()
                        .collect(Collectors.toList());
                m.put("partitions", partitions);
                m.put("partitionCount", partitions.size());
                members.add(m);
            }
            result.put("members", members);

            // Offset'ы и lag
            var offsets = adminClient.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();

            // End offsets для расчёта lag
            Set<TopicPartition> tps = offsets.keySet();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = new HashMap<>();
            if (!tps.isEmpty()) {
                Map<TopicPartition, OffsetSpec> request = new HashMap<>();
                tps.forEach(tp -> request.put(tp, OffsetSpec.latest()));
                endOffsets = adminClient.listOffsets(request).all().get();
            }

            List<Map<String, Object>> offsetList = new ArrayList<>();
            long totalLag = 0;
            for (var entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata oam = entry.getValue();
                if (oam == null) continue;

                long currentOffset = oam.offset();
                long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : currentOffset;
                long lag = Math.max(0, endOffset - currentOffset);
                totalLag += lag;

                Map<String, Object> o = new LinkedHashMap<>();
                o.put("topic", tp.topic());
                o.put("partition", tp.partition());
                o.put("currentOffset", currentOffset);
                o.put("endOffset", endOffset);
                o.put("lag", lag);
                offsetList.add(o);
            }
            offsetList.sort(Comparator.comparing((Map<String, Object> m) -> (String) m.get("topic"))
                    .thenComparingInt(m -> (int) m.get("partition")));

            result.put("offsets", offsetList);
            result.put("totalLag", totalLag);

        } catch (Exception e) {
            log.error("Failed to describe group {}: {}", groupId, e.getMessage());
            result.put("error", e.getMessage());
        }

        return result;
    }

    private String shorten(String memberId) {
        if (memberId == null) return "—";
        if (memberId.length() > 40) return memberId.substring(0, 37) + "...";
        return memberId;
    }
}
