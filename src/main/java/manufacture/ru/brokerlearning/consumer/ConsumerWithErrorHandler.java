package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Демонстрационный consumer с обработкой ошибок и Dead Letter Topic.
 * Только логирует — не сохраняет и не транслирует (чтобы не дублировать MainConsumer).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ConsumerWithErrorHandler {

    @KafkaListener(topics = "learning-topic", groupId = "error-handler-group", containerFactory = "errorHandlingFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("ConsumerWithErrorHandler received: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        ack.acknowledge();
    }
}
