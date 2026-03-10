package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Демонстрационный batch consumer — обрабатывает сообщения пачками.
 * Только логирует — не сохраняет и не транслирует (чтобы не дублировать MainConsumer).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BatchConsumer {

    @KafkaListener(topics = "learning-topic", groupId = "batch-group", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("BatchConsumer received batch of {} records", records.size());
        for (ConsumerRecord<String, String> record : records) {
            log.debug("BatchConsumer processing: topic={}, partition={}, offset={}, key={}",
                    record.topic(), record.partition(), record.offset(), record.key());
        }
        ack.acknowledge();
        log.info("BatchConsumer acknowledged batch of {} records", records.size());
    }
}
