package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import manufacture.ru.brokerlearning.service.RealtimeMessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class BatchConsumer {

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;

    @KafkaListener(topics = "learning-topic", groupId = "batch-group", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("BatchConsumer received batch of {} records", records.size());
        for (ConsumerRecord<String, String> record : records) {
            log.debug("BatchConsumer processing: topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            messageHistoryService.saveReceivedMessage(record);
            realtimeMessageService.broadcast(record);
        }
        ack.acknowledge();
        log.info("BatchConsumer acknowledged batch of {} records", records.size());
    }
}
