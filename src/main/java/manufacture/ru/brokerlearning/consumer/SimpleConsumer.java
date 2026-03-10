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

    private static final Set<String> SEMANTICS_TOPICS = Set.of(
            "at-most-once-topic", "at-least-once-topic", "exactly-once-topic",
            "benchmark-topic", "replay-topic", "compare-topic",
            "dlq-compare-topic", "dlq-compare-topic.DLT",
            "ordering-1p-topic", "ordering-5p-topic",
            "compression-none", "compression-gzip", "compression-snappy",
            "compression-lz4", "compression-zstd",
            "rebalancing-topic"
    );

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;

    @KafkaListener(topicPattern = ".*", groupId = "main-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ack.acknowledge();
        log.info("MainConsumer received: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        // Сохраняем в базу все сообщения (для мониторинга)
        messageHistoryService.saveReceivedMessage(record);
        // В ленту Consumer-страницы транслируем только пользовательские topics
        if (!SEMANTICS_TOPICS.contains(record.topic())) {
            realtimeMessageService.broadcast(record);
        }
    }
}
