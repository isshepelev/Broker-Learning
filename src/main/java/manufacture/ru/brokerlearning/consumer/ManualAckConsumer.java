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
public class ManualAckConsumer {

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;

    @KafkaListener(topics = "learning-topic", groupId = "manual-ack-group", containerFactory = "manualAckFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("ManualAckConsumer received: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        messageHistoryService.saveReceivedMessage(record);
        realtimeMessageService.broadcast(record);
        ack.acknowledge();
        log.info("ManualAckConsumer acknowledged offset={}", record.offset());
    }
}
