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

    public void saveSentMessage(String topic, String key, String value, Integer partition, String headers) {
        log.info("Saving sent message to topic: {}", topic);

        KafkaMessageEntity entity = KafkaMessageEntity.builder()
                .topic(topic)
                .messageKey(key)
                .messageValue(value)
                .partitionNum(partition)
                .headers(headers)
                .direction("SENT")
                .timestamp(LocalDateTime.now())
                .build();

        kafkaMessageRepository.save(entity);
        log.debug("Sent message saved successfully");
    }

    public void saveReceivedMessage(ConsumerRecord<String, String> record) {
        log.info("Saving received message from topic: {}, partition: {}, offset: {}",
                record.topic(), record.partition(), record.offset());

        KafkaMessageEntity entity = KafkaMessageEntity.builder()
                .topic(record.topic())
                .messageKey(record.key())
                .messageValue(record.value())
                .partitionNum(record.partition())
                .offsetNum(record.offset())
                .direction("RECEIVED")
                .timestamp(LocalDateTime.now())
                .build();

        kafkaMessageRepository.save(entity);
        log.debug("Received message saved successfully");
    }

    public List<KafkaMessageEntity> getRecentMessages() {
        log.info("Fetching recent messages");
        return kafkaMessageRepository.findTop100ByOrderByTimestampDesc();
    }

    public List<KafkaMessageEntity> getMessagesByTopic(String topic) {
        log.info("Fetching messages for topic: {}", topic);
        return kafkaMessageRepository.findByTopicOrderByTimestampDesc(topic);
    }

    public List<KafkaMessageEntity> getMessagesByDirection(String direction) {
        log.info("Fetching messages with direction: {}", direction);
        return kafkaMessageRepository.findByDirectionOrderByTimestampDesc(direction);
    }

    public List<KafkaMessageEntity> getMessagesReceivedSinceStart() {
        log.info("Fetching messages received since app start: {}", appStartTime);
        return kafkaMessageRepository.findByDirectionAndTimestampAfterOrderByTimestampDesc("RECEIVED", appStartTime);
    }
}
