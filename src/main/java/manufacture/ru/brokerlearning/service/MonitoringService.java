package manufacture.ru.brokerlearning.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class MonitoringService {

    private final AdminClient adminClient;
    private final MessageHistoryService historyService;
    private final UserResourceRepository resourceRepository;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    public Map<String, Object> getMessageThroughput(String sid, int minutes) {
        LocalDateTime now = LocalDateTime.now();
        int totalSlots = minutes * 6;

        int nowSec = now.getSecond();
        int roundedSec = (nowSec / 10) * 10;
        LocalDateTime currentSlot = now.withSecond(roundedSec).withNano(0);
        LocalDateTime firstSlot = currentSlot.minusSeconds((long)(totalSlots - 1) * 10);

        List<LocalDateTime> slots = new ArrayList<>();
        for (int i = 0; i < totalSlots; i++) {
            slots.add(firstSlot.plusSeconds((long) i * 10));
        }

        List<KafkaMessageEntity> messages = historyService.getMessagesSince(sid, firstSlot);

        int[][] counts = new int[totalSlots][2];
        for (KafkaMessageEntity msg : messages) {
            if (msg.getTimestamp() == null) continue;
            int msgSec = msg.getTimestamp().getSecond();
            int msgRounded = (msgSec / 10) * 10;
            LocalDateTime bucket = msg.getTimestamp().withSecond(msgRounded).withNano(0);

            for (int i = 0; i < totalSlots; i++) {
                if (slots.get(i).equals(bucket)) {
                    if ("SENT".equals(msg.getDirection())) counts[i][0]++;
                    else counts[i][1]++;
                    break;
                }
            }
        }

        List<String> labels = new ArrayList<>();
        List<Integer> sent = new ArrayList<>();
        List<Integer> received = new ArrayList<>();
        for (int i = 0; i < totalSlots; i++) {
            labels.add(slots.get(i).format(TIME_FMT));
            sent.add(counts[i][0]);
            received.add(counts[i][1]);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("labels", labels);
        result.put("sent", sent);
        result.put("received", received);
        return result;
    }

    public List<Map<String, Object>> getConsumerGroupLags(String sid) {
        Set<String> userGroups = resourceRepository.groupNamesForUser(sid);
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups().all().get();

            for (ConsumerGroupListing group : groups) {
                String groupId = group.groupId();
                if (!userGroups.contains(groupId)) continue;
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets =
                            adminClient.listConsumerGroupOffsets(groupId)
                                    .partitionsToOffsetAndMetadata().get();
                    if (offsets.isEmpty()) continue;

                    Map<TopicPartition, OffsetSpec> request = offsets.keySet().stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                            adminClient.listOffsets(request).all().get();

                    long totalLag = 0, totalCommitted = 0, totalEnd = 0;
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        long committed = entry.getValue().offset();
                        ListOffsetsResult.ListOffsetsResultInfo endInfo = endOffsets.get(entry.getKey());
                        long end = endInfo != null ? endInfo.offset() : committed;
                        totalLag += Math.max(0, end - committed);
                        totalCommitted += committed;
                        totalEnd += end;
                    }

                    Map<String, Object> info = new HashMap<>();
                    info.put("groupId", groupId);
                    info.put("lag", totalLag);
                    info.put("committed", totalCommitted);
                    info.put("endOffset", totalEnd);
                    info.put("partitions", offsets.size());
                    result.add(info);
                } catch (Exception e) {
                    log.debug("Skip group {}: {}", groupId, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to get consumer group lags: {}", e.getMessage());
        }
        result.sort((a, b) -> Long.compare((long) b.get("lag"), (long) a.get("lag")));
        return result;
    }

    public List<Map<String, Object>> getTopicSizes(String sid) {
        Set<String> userTopics = resourceRepository.topicNamesForUser(sid);
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Set<String> topics = adminClient.listTopics().names().get();

            for (String topic : topics) {
                if (!userTopics.contains(topic)) continue;
                try {
                    var desc = adminClient.describeTopics(Collections.singletonList(topic))
                            .topicNameValues().get(topic).get();

                    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
                    for (var p : desc.partitions()) {
                        request.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
                    }
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                            adminClient.listOffsets(request).all().get();

                    long totalMessages = endOffsets.values().stream()
                            .mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset).sum();

                    Map<String, Object> info = new HashMap<>();
                    info.put("topic", topic);
                    info.put("partitions", desc.partitions().size());
                    info.put("totalMessages", totalMessages);
                    result.add(info);
                } catch (Exception e) {
                    log.debug("Skip topic {}: {}", topic, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to get topic sizes: {}", e.getMessage());
        }
        result.sort((a, b) -> Long.compare((long) b.get("totalMessages"), (long) a.get("totalMessages")));
        return result;
    }

    public Map<String, Object> getOverallStats(String sid) {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalSent", historyService.countByDirection("SENT", sid));
        stats.put("totalReceived", historyService.countByDirection("RECEIVED", sid));
        stats.put("lastMinute", historyService.countSinceTime(sid, LocalDateTime.now().minusMinutes(1)));
        stats.put("last5Minutes", historyService.countSinceTime(sid, LocalDateTime.now().minusMinutes(5)));

        Set<String> userTopics = resourceRepository.topicNamesForUser(sid);
        stats.put("topicsCount", userTopics.size());

        Set<String> userGroups = resourceRepository.groupNamesForUser(sid);
        stats.put("consumerGroupsCount", userGroups.size());

        return stats;
    }
}
