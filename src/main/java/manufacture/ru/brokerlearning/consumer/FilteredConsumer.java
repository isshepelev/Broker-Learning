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
public class FilteredConsumer {

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;

    @KafkaListener(topics = "learning-topic", groupId = "filtered-group", containerFactory = "filteredFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("FilteredConsumer received (non-skip): topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        messageHistoryService.saveReceivedMessage(record);
        realtimeMessageService.broadcast(record);
        ack.acknowledge();
        log.debug("FilteredConsumer acknowledged offset={}", record.offset());
    }
}
