package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import manufacture.ru.brokerlearning.config.InternalKafkaRegistry;

import java.util.*;
import java.util.Set;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/consumer-groups")
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final AdminClient adminClient;
    private final UserSessionHelper sessionHelper;
    private final UserResourceRepository resourceRepository;

    @GetMapping("")
    public String consumerGroupsPage(Model model) {
        model.addAttribute("currentPage", "consumer-groups");
        return "consumer-groups";
    }

    @GetMapping("/list")
    @ResponseBody
    public List<Map<String, Object>> listGroups() {
        try {
            String sid = sessionHelper.currentSid();
            Set<String> userGroups = resourceRepository.groupNamesForUser(sid);

            var listings = adminClient.listConsumerGroups().all().get();
            var groupIds = listings.stream()
                    .map(ConsumerGroupListing::groupId)
                    .filter(g -> InternalKafkaRegistry.isUserGroup(g) && userGroups.contains(g))
                    .collect(Collectors.toList());

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
        // Проверяем что группа принадлежит текущему пользователю
        Set<String> userGroups = resourceRepository.groupNamesForUser(sessionHelper.currentSid());
        if (!userGroups.contains(groupId)) {
            return Map.of("error", "Consumer group не принадлежит вам");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupId", groupId);

        try {
            var desc = adminClient.describeConsumerGroups(List.of(groupId)).all().get().get(groupId);
            result.put("state", desc.state().toString());
            result.put("partitionAssignor", desc.partitionAssignor());
            result.put("coordinator", desc.coordinator().host() + ":" + desc.coordinator().port());

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

            var offsets = adminClient.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();

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
