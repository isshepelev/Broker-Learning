package manufacture.ru.brokerlearning.service;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.KafkaConsumerConfig;
import manufacture.ru.brokerlearning.config.RabbitConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Сервис для сравнения Dead Letter Queue в Kafka (DLT) и RabbitMQ (DLX).
 */
@Service
@Slf4j
public class DlqCompareService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String KAFKA_TOPIC = "dlq-compare-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final KafkaConsumerConfig kafkaConsumerConfig;

    @Getter
    private final List<Map<String, String>> kafkaEvents = Collections.synchronizedList(new ArrayList<>());
    @Getter
    private final List<Map<String, String>> rabbitEvents = Collections.synchronizedList(new ArrayList<>());
    @Getter
    private final List<Map<String, String>> kafkaDead = Collections.synchronizedList(new ArrayList<>());
    @Getter
    private final List<Map<String, String>> rabbitDead = Collections.synchronizedList(new ArrayList<>());

    public DlqCompareService(KafkaTemplate<String, String> kafkaTemplate,
                             RabbitTemplate rabbitTemplate,
                             KafkaConsumerConfig kafkaConsumerConfig) {
        this.kafkaTemplate = kafkaTemplate;
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    @PostConstruct
    public void init() {
        // Регистрируем callback — когда errorHandler исчерпает retry, он вызовет нас
        kafkaConsumerConfig.setDltCallback((record, exception) -> {
            String value = record.value() != null ? record.value().toString() : "";
            String time = now();
            addKafkaEvent(time, "DLT", value, "Попало в Dead Letter Topic после 3 retry");
            addDead(kafkaDead, time, value, "Kafka DLT");
            log.warn("DLQ Kafka DLT callback: message buried: {}", value);
        });
    }

    public void sendToBoth(String message) {
        String time = now();

        // Kafka
        kafkaTemplate.send(KAFKA_TOPIC, "dlq-key", message);
        kafkaTemplate.flush();
        addKafkaEvent(time, "SENT", message, "Отправлено в dlq-compare-topic");

        // RabbitMQ
        rabbitTemplate.convertAndSend(RabbitConfig.DLQ_WORK_EXCHANGE, RabbitConfig.DLQ_WORK_KEY, message);
        addRabbitEvent(time, "SENT", message, "Отправлено в dlq-work-queue");

        log.info("DLQ Compare: sent '{}' to both brokers", message);
    }

    // ======================== KAFKA ========================

    @KafkaListener(topics = "dlq-compare-topic", groupId = "dlq-compare-group", containerFactory = "errorHandlingFactory")
    public void kafkaConsume(ConsumerRecord<String, String> record) {
        String value = record.value() != null ? record.value() : "";
        String time = now();

        if (isBadMessage(value)) {
            addKafkaEvent(time, "RETRY", value, "Попытка обработки — ошибка! Retry...");
            log.warn("DLQ Kafka: BAD message, throwing exception: {}", value);
            throw new RuntimeException("Kafka: ошибка обработки [" + value + "]");
        }

        addKafkaEvent(time, "OK", value, "Успешно обработано");
        log.info("DLQ Kafka: processed OK: {}", value);
    }

    // DLT consumer больше не нужен — callback в errorHandler напрямую записывает событие

    // ======================== RABBITMQ ========================

    @RabbitListener(queues = RabbitConfig.DLQ_WORK_QUEUE)
    public void rabbitConsume(String message) {
        String time = now();

        if (isBadMessage(message)) {
            addRabbitEvent(time, "FAIL", message, "Ошибка обработки — reject → сразу в DLX");
            log.warn("DLQ Rabbit: BAD message, rejecting: {}", message);
            throw new org.springframework.amqp.AmqpRejectAndDontRequeueException(
                    "RabbitMQ: ошибка обработки [" + message + "]");
        }

        addRabbitEvent(time, "OK", message, "Успешно обработано");
        log.info("DLQ Rabbit: processed OK: {}", message);
    }

    @RabbitListener(queues = RabbitConfig.DLQ_DEAD_QUEUE)
    public void rabbitDlqConsume(String message) {
        String time = now();
        addRabbitEvent(time, "DLQ", message, "Попало в Dead Letter Queue (мгновенно после reject)");
        addDead(rabbitDead, time, message, "RabbitMQ DLQ");
        log.warn("DLQ Rabbit DLQ: message buried: {}", message);
    }

    // ======================== Helpers ========================

    public void clear() {
        kafkaEvents.clear();
        rabbitEvents.clear();
        kafkaDead.clear();
        rabbitDead.clear();
    }

    private boolean isBadMessage(String value) {
        String lower = value.toLowerCase();
        return lower.contains("poison") || lower.contains("bad") || lower.contains("error")
                || lower.contains("fail") || lower.contains("crash") || lower.contains("broken");
    }

    private void addKafkaEvent(String time, String status, String value, String description) {
        kafkaEvents.add(0, Map.of("time", time, "status", status, "value", value, "desc", description));
        trimList(kafkaEvents);
    }

    private void addRabbitEvent(String time, String status, String value, String description) {
        rabbitEvents.add(0, Map.of("time", time, "status", status, "value", value, "desc", description));
        trimList(rabbitEvents);
    }

    private void addDead(List<Map<String, String>> list, String time, String value, String source) {
        list.add(0, Map.of("time", time, "value", value, "source", source));
        trimList(list);
    }

    private void trimList(List<?> list) {
        while (list.size() > 100) list.remove(list.size() - 1);
    }

    private String now() {
        return LocalDateTime.now().format(FMT);
    }
}
