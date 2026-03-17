package manufacture.ru.brokerlearning.delivery;
import manufacture.ru.brokerlearning.config.UserSessionHelper;

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

    public Map<String, Object> demonstrate(String sid, int messageCount) {
        String topic = UserSessionHelper.isAdminSid(sid) ? "at-most-once-topic" : "at-most-once-" + sid;
        Map<String, Object> results = new HashMap<>();
        int sent = 0;

        for (int i = 0; i < messageCount; i++) {
            String message = "at-most-once-message-" + i;
            kafkaTemplate.send(topic, message);
            historyService.saveSentMessage(topic, null, message, null, null, sid);
            sent++;
        }

        results.put("sent", sent);
        results.put("description",
                "At-most-once delivery: messages are sent without waiting for acknowledgement. " +
                "Consumer uses auto-commit, so messages may be lost but never reprocessed.");
        return results;
    }
}
