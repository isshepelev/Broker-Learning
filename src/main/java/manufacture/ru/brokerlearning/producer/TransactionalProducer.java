package manufacture.ru.brokerlearning.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TransactionalProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionalProducer(@Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendInTransaction(String topic1, String key, String value1, String topic2, String value2) {
        log.info("Sending in transaction: topic1={}, topic2={}, key={}", topic1, topic2, key);
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(topic1, key, value1);
            log.info("Sent first message in transaction: topic={}, key={}, value={}", topic1, key, value1);
            operations.send(topic2, key, value2);
            log.info("Sent second message in transaction: topic={}, key={}, value={}", topic2, key, value2);
            return true;
        });
        log.info("Transaction completed successfully");
    }
}
