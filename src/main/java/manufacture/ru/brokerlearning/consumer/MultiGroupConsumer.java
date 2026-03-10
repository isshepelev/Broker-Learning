package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Демонстрация: два listener-а в разных consumer groups читают один и тот же topic.
 * Только логирует — не сохраняет и не транслирует (чтобы не дублировать MainConsumer).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MultiGroupConsumer {

    @KafkaListener(topics = "learning-topic", groupId = "analytics-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenAnalytics(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("MultiGroupConsumer [analytics-group] received: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        ack.acknowledge();
    }

    @KafkaListener(topics = "learning-topic", groupId = "notification-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenNotification(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("MultiGroupConsumer [notification-group] received: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        ack.acknowledge();
    }
}
