package manufacture.ru.brokerlearning.delivery;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AtMostOnceService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final manufacture.ru.brokerlearning.service.MessageHistoryService historyService;

    public Map<String, Object> demonstrate(int messageCount) {
        Map<String, Object> results = new HashMap<>();
        int sent = 0;

        for (int i = 0; i < messageCount; i++) {
            String message = "at-most-once-message-" + i;
            kafkaTemplate.send("at-most-once-topic", message);
            historyService.saveSentMessage("at-most-once-topic", null, message, null, null);
            sent++;
            log.info("Sent (fire-and-forget): {}", message);
        }

        results.put("sent", sent);
        results.put("description",
                "At-most-once delivery: messages are sent without waiting for acknowledgement. " +
                "Consumer uses auto-commit, so messages may be lost but never reprocessed.");
        return results;
    }
}
