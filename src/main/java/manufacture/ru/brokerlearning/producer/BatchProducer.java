package manufacture.ru.brokerlearning.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.MessageDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class BatchProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public List<CompletableFuture<SendResult<String, String>>> sendBatch(String topic, List<MessageDto> messages) {
        log.info("Sending batch of {} messages to topic={}", messages.size(), topic);
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();
        for (MessageDto message : messages) {
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, message.getKey(), message.getValue());
            futures.add(future);
            log.debug("Queued message: key={}, value={}", message.getKey(), message.getValue());
        }
        log.info("Batch of {} messages queued for topic={}", messages.size(), topic);
        return futures;
    }
}
