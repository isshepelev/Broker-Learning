package manufacture.ru.brokerlearning.delivery;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class AtLeastOnceService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Map<String, Object> demonstrate(int messageCount) {
        Map<String, Object> results = new HashMap<>();
        int sent = 0;
        int confirmed = 0;

        for (int i = 0; i < messageCount; i++) {
            String message = "at-least-once-message-" + i;
            try {
                CompletableFuture<SendResult<String, String>> future =
                        kafkaTemplate.send("at-least-once-topic", message);
                SendResult<String, String> result = future.get();
                sent++;
                confirmed++;
                log.info("Sent and confirmed: {} -> partition={}, offset={}",
                        message,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } catch (Exception e) {
                sent++;
                log.error("Failed to confirm message: {}", message, e);
            }
        }

        results.put("sent", sent);
        results.put("confirmed", confirmed);
        results.put("description",
                "At-least-once delivery: producer waits for broker acknowledgement via future.get(). " +
                "Consumer commits offset after processing. Messages are never lost but may be reprocessed on failure.");
        return results;
    }
}
