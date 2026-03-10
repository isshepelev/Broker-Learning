package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import manufacture.ru.brokerlearning.service.RealtimeMessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Главный consumer — слушает ВСЕ topics по шаблону.
 * Сохраняет сообщения в историю и транслирует через SSE в ленту Consumer-страницы.
 * Игнорирует служебные topics семантик доставки, чтобы не засорять ленту.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SimpleConsumer {

    private static final Set<String> IGNORED_TOPICS = Set.of(
            "at-most-once-topic", "at-least-once-topic", "exactly-once-topic"
    );

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;

    @KafkaListener(topicPattern = ".*", groupId = "main-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ack.acknowledge();
        if (IGNORED_TOPICS.contains(record.topic())) {
            return;
        }
        log.info("MainConsumer received: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        messageHistoryService.saveReceivedMessage(record);
        realtimeMessageService.broadcast(record);
    }
}
