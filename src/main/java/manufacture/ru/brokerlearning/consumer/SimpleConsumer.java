package manufacture.ru.brokerlearning.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.InternalKafkaRegistry;
import manufacture.ru.brokerlearning.model.UserResource;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import manufacture.ru.brokerlearning.service.RealtimeMessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Главный consumer — слушает ВСЕ topics по шаблону.
 * Определяет владельца топика и сохраняет сообщение с ownerSid.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SimpleConsumer {

    private final MessageHistoryService messageHistoryService;
    private final RealtimeMessageService realtimeMessageService;
    private final UserResourceRepository resourceRepository;

    @KafkaListener(topicPattern = ".*", groupId = "main-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ack.acknowledge();

        String topic = record.topic();

        // Определяем владельца топика
        String ownerSid = resourceRepository.findByResourceTypeAndResourceName("TOPIC", topic)
                .map(UserResource::getOwnerSid)
                .orElse(null);

        // Сохраняем в базу с привязкой к владельцу
        messageHistoryService.saveReceivedMessage(record, ownerSid);

        // В SSE транслируем только пользовательские топики (не internal/per-user demo)
        if (!InternalKafkaRegistry.isInternalTopic(topic) && ownerSid != null) {
            realtimeMessageService.broadcast(record, ownerSid);
        }
    }
}
