package manufacture.ru.brokerlearning.delivery;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class ExactlyOnceService {

    private final KafkaTemplate<String, String> transactionalKafkaTemplate;

    public ExactlyOnceService(
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, String> transactionalKafkaTemplate) {
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
    }

    public Map<String, Object> demonstrate(int messageCount) {
        Map<String, Object> results = new HashMap<>();

        int sent = transactionalKafkaTemplate.executeInTransaction(operations -> {
            int count = 0;
            for (int i = 0; i < messageCount; i++) {
                String message = "exactly-once-message-" + i;
                operations.send("exactly-once-topic", message);
                count++;
                log.info("Sent (transactional): {}", message);
            }
            return count;
        });

        results.put("sent", sent);
        results.put("transactional", true);
        results.put("description",
                "Exactly-once delivery: messages are sent inside a Kafka transaction using executeInTransaction. " +
                "Combined with idempotent producer and transactional consumer, each message is delivered exactly once.");
        return results;
    }
}
