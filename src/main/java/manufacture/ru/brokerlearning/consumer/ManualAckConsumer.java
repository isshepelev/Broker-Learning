package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Демонстрационный consumer с ручным подтверждением offset-а.
 * Только логирует — не сохраняет и не транслирует (чтобы не дублировать MainConsumer).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ManualAckConsumer {

    @KafkaListener(topics = "learning-topic", groupId = "manual-ack-group", containerFactory = "manualAckFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("ManualAckConsumer received: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        // Демонстрация: offset коммитится только после явного вызова acknowledge()
        ack.acknowledge();
        log.debug("ManualAckConsumer acknowledged offset={}", record.offset());
    }
}
