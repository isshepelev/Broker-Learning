package manufacture.ru.brokerlearning.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class SimpleProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendFireAndForget(String topic, String key, String value) {
        log.info("Fire-and-forget sending to topic={}, key={}, value={}", topic, key, value);
        kafkaTemplate.send(topic, key, value);
    }

    public String sendSync(String topic, String key, String value)
            throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Sync sending to topic={}, key={}, value={}", topic, key, value);
        SendResult<String, String> result = kafkaTemplate.send(topic, key, value).get(10, TimeUnit.SECONDS);
        String info = String.format("Message sent successfully: topic=%s, partition=%d, offset=%d, timestamp=%d",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                result.getRecordMetadata().timestamp());
        log.info(info);
        return info;
    }
}
