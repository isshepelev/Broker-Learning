package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import manufacture.ru.brokerlearning.service.RealtimeMessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MultiGroupConsumer {

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;

    @KafkaListener(topics = "learning-topic", groupId = "analytics-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenAnalytics(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("MultiGroupConsumer [analytics-group] received: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        messageHistoryService.saveReceivedMessage(record);
        realtimeMessageService.broadcast(record);
        ack.acknowledge();
        log.debug("MultiGroupConsumer [analytics-group] acknowledged offset={}", record.offset());
    }

    @KafkaListener(topics = "learning-topic", groupId = "notification-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenNotification(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("MultiGroupConsumer [notification-group] received: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        messageHistoryService.saveReceivedMessage(record);
        realtimeMessageService.broadcast(record);
        ack.acknowledge();
        log.debug("MultiGroupConsumer [notification-group] acknowledged offset={}", record.offset());
    }
}
