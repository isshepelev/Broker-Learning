package manufacture.ru.brokerlearning.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.repository.KafkaMessageRepository;
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
    private final KafkaMessageRepository messageRepository;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Сообщений за последние N минут, сгруппированные по 10-секундным интервалам.
     * Всегда возвращает ровно (minutes * 6) слотов, привязанных к "круглым" 10-секундным меткам.
     */
    public Map<String, Object> getMessageThroughput(int minutes) {
        LocalDateTime now = LocalDateTime.now();
        int totalSlots = minutes * 6; // 6 слотов по 10 сек в минуте

        // Начало первого слота: округляем now вниз до 10 сек и отступаем назад
        int nowSec = now.getSecond();
        int roundedSec = (nowSec / 10) * 10;
        LocalDateTime currentSlot = now.withSecond(roundedSec).withNano(0);
        LocalDateTime firstSlot = currentSlot.minusSeconds((long)(totalSlots - 1) * 10);

        // Создаём все слоты
        List<LocalDateTime> slots = new ArrayList<>();
        for (int i = 0; i < totalSlots; i++) {
            slots.add(firstSlot.plusSeconds((long) i * 10));
        }

        // Загружаем сообщения
        List<KafkaMessageEntity> messages = messageRepository.findByTimestampAfterOrderByTimestampAsc(firstSlot);

        // Считаем по слотам
        int[][] counts = new int[totalSlots][2]; // [sent, received]
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

    /**
     * Lag по каждому consumer group
     */
    public List<Map<String, Object>> getConsumerGroupLags() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups().all().get();

            for (ConsumerGroupListing group : groups) {
                String groupId = group.groupId();
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets =
                            adminClient.listConsumerGroupOffsets(groupId)
                                    .partitionsToOffsetAndMetadata().get();

                    if (offsets.isEmpty()) continue;

                    // Получаем end offsets
                    Map<TopicPartition, OffsetSpec> request = offsets.keySet().stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                            adminClient.listOffsets(request).all().get();

                    long totalLag = 0;
                    long totalCommitted = 0;
                    long totalEnd = 0;

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

    /**
     * Размеры topic-ов (сумма end offset по всем partitions)
     */
    public List<Map<String, Object>> getTopicSizes() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Set<String> topics = adminClient.listTopics().names().get();

            for (String topic : topics) {
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
                            .mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset)
                            .sum();

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

    /**
     * Общая статистика
     */
    public Map<String, Object> getOverallStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalSent", messageRepository.countByDirection("SENT"));
        stats.put("totalReceived", messageRepository.countByDirection("RECEIVED"));
        stats.put("lastMinute", messageRepository.countByTimestampAfter(LocalDateTime.now().minusMinutes(1)));
        stats.put("last5Minutes", messageRepository.countByTimestampAfter(LocalDateTime.now().minusMinutes(5)));

        try {
            stats.put("topicsCount", adminClient.listTopics().names().get().size());
        } catch (Exception e) {
            stats.put("topicsCount", 0);
        }

        try {
            stats.put("consumerGroupsCount", adminClient.listConsumerGroups().all().get().size());
        } catch (Exception e) {
            stats.put("consumerGroupsCount", 0);
        }

        return stats;
    }
}
