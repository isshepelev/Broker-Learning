package manufacture.ru.brokerlearning.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.repository.KafkaMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageHistoryService {

    private final KafkaMessageRepository kafkaMessageRepository;
    private final LocalDateTime appStartTime = LocalDateTime.now();

    public void saveSentMessage(String topic, String key, String value, Integer partition, String headers, String ownerSid) {
        KafkaMessageEntity entity = KafkaMessageEntity.builder()
                .topic(topic)
                .messageKey(key)
                .messageValue(value)
                .partitionNum(partition)
                .headers(headers)
                .direction("SENT")
                .ownerSid(ownerSid)
                .timestamp(LocalDateTime.now())
                .build();
        kafkaMessageRepository.save(entity);
    }

    public void saveReceivedMessage(ConsumerRecord<String, String> record, String ownerSid) {
        KafkaMessageEntity entity = KafkaMessageEntity.builder()
                .topic(record.topic())
                .messageKey(record.key())
                .messageValue(record.value())
                .partitionNum(record.partition())
                .offsetNum(record.offset())
                .direction("RECEIVED")
                .ownerSid(ownerSid)
                .timestamp(LocalDateTime.now())
                .build();
        kafkaMessageRepository.save(entity);
    }

    public List<KafkaMessageEntity> getRecentMessages(String ownerSid) {
        return kafkaMessageRepository.findTop100ByOwnerSidOrderByTimestampDesc(ownerSid);
    }

    public List<KafkaMessageEntity> getMessagesByTopic(String topic) {
        return kafkaMessageRepository.findByTopicOrderByTimestampDesc(topic);
    }

    public List<KafkaMessageEntity> getMessagesByDirection(String direction, String ownerSid) {
        return kafkaMessageRepository.findByDirectionAndOwnerSidOrderByTimestampDesc(direction, ownerSid);
    }

    public List<KafkaMessageEntity> getMessagesReceivedSinceStart(String ownerSid) {
        return kafkaMessageRepository.findByOwnerSidAndDirectionAndTimestampAfterOrderByTimestampDesc(
                ownerSid, "RECEIVED", appStartTime);
    }

    public long countByDirection(String direction, String ownerSid) {
        return kafkaMessageRepository.countByDirectionAndOwnerSid(direction, ownerSid);
    }

    public long countSinceTime(String ownerSid, LocalDateTime after) {
        return kafkaMessageRepository.countByOwnerSidAndTimestampAfter(ownerSid, after);
    }

    public List<KafkaMessageEntity> getMessagesSince(String ownerSid, LocalDateTime after) {
        return kafkaMessageRepository.findByOwnerSidAndTimestampAfterOrderByTimestampAsc(ownerSid, after);
    }
}
