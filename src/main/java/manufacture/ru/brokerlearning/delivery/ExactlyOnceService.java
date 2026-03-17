package manufacture.ru.brokerlearning.delivery;
import manufacture.ru.brokerlearning.config.UserSessionHelper;

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
    private final manufacture.ru.brokerlearning.service.MessageHistoryService historyService;

    public ExactlyOnceService(
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, String> transactionalKafkaTemplate,
            manufacture.ru.brokerlearning.service.MessageHistoryService historyService) {
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
        this.historyService = historyService;
    }

    public Map<String, Object> demonstrate(String sid, int messageCount) {
        String topic = UserSessionHelper.isAdminSid(sid) ? "exactly-once-topic" : "exactly-once-" + sid;
        Map<String, Object> results = new HashMap<>();

        int sent = transactionalKafkaTemplate.executeInTransaction(operations -> {
            int count = 0;
            for (int i = 0; i < messageCount; i++) {
                String message = "exactly-once-message-" + i;
                operations.send(topic, message);
                historyService.saveSentMessage(topic, null, message, null, null, sid);
                count++;
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
