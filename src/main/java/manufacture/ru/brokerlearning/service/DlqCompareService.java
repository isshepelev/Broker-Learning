package manufacture.ru.brokerlearning.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.KafkaConsumerConfig;
import manufacture.ru.brokerlearning.config.RabbitConfig;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.repository.KafkaMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class DlqCompareService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String KAFKA_TOPIC = "dlq-compare-topic";
    private static final String SID_SEPARATOR = "::";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final KafkaMessageRepository messageRepository;

    private static class DlqSession {
        final List<Map<String, String>> kafkaEvents = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, String>> rabbitEvents = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, String>> kafkaDead = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, String>> rabbitDead = Collections.synchronizedList(new ArrayList<>());
    }

    private final ConcurrentHashMap<String, DlqSession> sessions = new ConcurrentHashMap<>();

    public DlqCompareService(KafkaTemplate<String, String> kafkaTemplate,
                             RabbitTemplate rabbitTemplate,
                             KafkaConsumerConfig kafkaConsumerConfig,
                             KafkaMessageRepository messageRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.messageRepository = messageRepository;
    }

    private DlqSession session(String sid) {
        return sessions.computeIfAbsent(sid, k -> new DlqSession());
    }

    @PostConstruct
    public void init() {
        kafkaConsumerConfig.setDltCallback((record, exception) -> {
            String key = record.key() != null ? record.key().toString() : "";
            String value = record.value() != null ? record.value().toString() : "";
            String sid = extractSid(key);
            String time = now();
            DlqSession s = session(sid);
            addEvent(s.kafkaEvents, time, "DLT", value, "Попало в Dead Letter Topic после 3 retry");
            addDead(s.kafkaDead, time, value);
        });
    }

    public void sendToBoth(String sid, String message) {
        String time = now();
        String key = sid + SID_SEPARATOR + "dlq-key";
        DlqSession s = session(sid);

        kafkaTemplate.send(KAFKA_TOPIC, key, message);
        kafkaTemplate.flush();
        addEvent(s.kafkaEvents, time, "SENT", message, "Отправлено в dlq-compare-topic");

        // RabbitMQ: embed sid in message as prefix
        rabbitTemplate.convertAndSend(RabbitConfig.DLQ_WORK_EXCHANGE, RabbitConfig.DLQ_WORK_KEY, sid + SID_SEPARATOR + message);
        addEvent(s.rabbitEvents, time, "SENT", message, "Отправлено в dlq-work-queue");
    }

    // ======================== KAFKA ========================

    @KafkaListener(topics = "dlq-compare-topic", groupId = "dlq-compare-group", containerFactory = "errorHandlingFactory")
    public void kafkaConsume(ConsumerRecord<String, String> record) {
        String key = record.key() != null ? record.key() : "";
        String value = record.value() != null ? record.value() : "";
        String sid = extractSid(key);
        String time = now();
        DlqSession s = session(sid);

        if (isBadMessage(value)) {
            addEvent(s.kafkaEvents, time, "RETRY", value, "Попытка обработки — ошибка! Retry...");
            throw new RuntimeException("Kafka: ошибка обработки [" + value + "]");
        }
        addEvent(s.kafkaEvents, time, "OK", value, "Успешно обработано");
    }

    // ======================== RABBITMQ ========================

    @RabbitListener(queues = RabbitConfig.DLQ_WORK_QUEUE)
    public void rabbitConsume(String rawMessage) {
        String sid = extractSidFromRabbit(rawMessage);
        String message = extractMessageFromRabbit(rawMessage);
        String time = now();
        DlqSession s = session(sid);

        if (isBadMessage(message)) {
            addEvent(s.rabbitEvents, time, "FAIL", message, "Ошибка обработки — reject → сразу в DLX");
            throw new org.springframework.amqp.AmqpRejectAndDontRequeueException("RabbitMQ: ошибка [" + message + "]");
        }
        addEvent(s.rabbitEvents, time, "OK", message, "Успешно обработано");
    }

    @RabbitListener(queues = RabbitConfig.DLQ_DEAD_QUEUE)
    public void rabbitDlqConsume(String rawMessage) {
        String sid = extractSidFromRabbit(rawMessage);
        String message = extractMessageFromRabbit(rawMessage);
        String time = now();
        DlqSession s = session(sid);
        addEvent(s.rabbitEvents, time, "DLQ", message, "Попало в Dead Letter Queue (мгновенно после reject)");
        addDead(s.rabbitDead, time, message);
    }

    // ======================== Getters for controller ========================

    public List<Map<String, String>> getKafkaEvents(String sid) { return session(sid).kafkaEvents; }
    public List<Map<String, String>> getRabbitEvents(String sid) { return session(sid).rabbitEvents; }
    public List<Map<String, String>> getKafkaDead(String sid) { return session(sid).kafkaDead; }
    public List<Map<String, String>> getRabbitDead(String sid) { return session(sid).rabbitDead; }

    // ======================== Actions ========================

    public Map<String, Object> retryKafkaDead(String sid, int index) {
        DlqSession s = session(sid);
        if (index < 0 || index >= s.kafkaDead.size()) return Map.of("error", "Неверный индекс");
        Map<String, String> dead = s.kafkaDead.get(index);
        String value = dead.get("value");
        kafkaTemplate.send(KAFKA_TOPIC, sid + SID_SEPARATOR + "dlq-retry", value);
        kafkaTemplate.flush();
        s.kafkaDead.remove(index);
        addEvent(s.kafkaEvents, now(), "SENT", value, "Переотправлено из DLT (retry)");
        return Map.of("success", true);
    }

    public Map<String, Object> retryRabbitDead(String sid, int index) {
        DlqSession s = session(sid);
        if (index < 0 || index >= s.rabbitDead.size()) return Map.of("error", "Неверный индекс");
        Map<String, String> dead = s.rabbitDead.get(index);
        String value = dead.get("value");
        rabbitTemplate.convertAndSend(RabbitConfig.DLQ_WORK_EXCHANGE, RabbitConfig.DLQ_WORK_KEY, sid + SID_SEPARATOR + value);
        s.rabbitDead.remove(index);
        addEvent(s.rabbitEvents, now(), "SENT", value, "Переотправлено из DLQ (retry)");
        return Map.of("success", true);
    }

    public Map<String, Object> retryAllKafkaDead(String sid) {
        DlqSession s = session(sid);
        int count = s.kafkaDead.size();
        for (int i = count - 1; i >= 0; i--) retryKafkaDead(sid, 0);
        return Map.of("success", true, "count", count);
    }

    public Map<String, Object> retryAllRabbitDead(String sid) {
        DlqSession s = session(sid);
        int count = s.rabbitDead.size();
        for (int i = count - 1; i >= 0; i--) retryRabbitDead(sid, 0);
        return Map.of("success", true, "count", count);
    }

    public Map<String, Object> saveDeadToDb(String sid, String source, int index) {
        DlqSession s = session(sid);
        List<Map<String, String>> list = "kafka".equals(source) ? s.kafkaDead : s.rabbitDead;
        if (index < 0 || index >= list.size()) return Map.of("error", "Неверный индекс");
        saveSingleToDb(list.get(index), source, sid);
        list.remove(index);
        return Map.of("success", true);
    }

    public Map<String, Object> saveAllDeadToDb(String sid, String source) {
        DlqSession s = session(sid);
        List<Map<String, String>> list = "kafka".equals(source) ? s.kafkaDead : s.rabbitDead;
        int count = list.size();
        for (Map<String, String> dead : list) saveSingleToDb(dead, source, sid);
        list.clear();
        return Map.of("success", true, "count", count);
    }

    public Map<String, Object> deleteDeadMessage(String sid, String source, int index) {
        DlqSession s = session(sid);
        List<Map<String, String>> list = "kafka".equals(source) ? s.kafkaDead : s.rabbitDead;
        if (index < 0 || index >= list.size()) return Map.of("error", "Неверный индекс");
        list.remove(index);
        return Map.of("success", true);
    }

    public List<Map<String, Object>> getSavedDeadMessages(String ownerSid) {
        List<KafkaMessageEntity> entities = messageRepository.findByDirectionAndOwnerSidOrderByTimestampDesc("DLQ", ownerSid);
        List<Map<String, Object>> result = new ArrayList<>();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (KafkaMessageEntity e : entities) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", e.getId());
            m.put("source", e.getTopic());
            m.put("value", e.getMessageValue());
            m.put("time", e.getTimestamp() != null ? e.getTimestamp().format(fmt) : "");
            m.put("key", e.getMessageKey() != null ? e.getMessageKey() : "");
            m.put("direction", e.getDirection());
            m.put("headers", e.getHeaders() != null ? e.getHeaders() : "");
            m.put("groupId", e.getGroupId() != null ? e.getGroupId() : "");
            result.add(m);
        }
        return result;
    }

    public Map<String, Object> deleteSavedMessage(Long id) {
        messageRepository.deleteById(id);
        return Map.of("success", true);
    }

    public Map<String, Object> retrySavedMessage(String sid, Long id) {
        Optional<KafkaMessageEntity> opt = messageRepository.findById(id);
        if (opt.isEmpty()) return Map.of("error", "Не найдено");
        KafkaMessageEntity entity = opt.get();
        String value = entity.getMessageValue();
        DlqSession s = session(sid);

        if (entity.getTopic().contains("Kafka")) {
            kafkaTemplate.send(KAFKA_TOPIC, sid + SID_SEPARATOR + "dlq-retry", value);
            kafkaTemplate.flush();
            addEvent(s.kafkaEvents, now(), "SENT", value, "Переотправлено из БД (retry)");
        } else {
            rabbitTemplate.convertAndSend(RabbitConfig.DLQ_WORK_EXCHANGE, RabbitConfig.DLQ_WORK_KEY, sid + SID_SEPARATOR + value);
            addEvent(s.rabbitEvents, now(), "SENT", value, "Переотправлено из БД (retry)");
        }
        messageRepository.deleteById(id);
        return Map.of("success", true);
    }

    public void clear(String sid) {
        sessions.remove(sid);
    }

    public void cleanupSession(String sid) {
        sessions.remove(sid);
    }

    // ======================== Helpers ========================

    private boolean isBadMessage(String value) {
        String lower = value.toLowerCase();
        return lower.contains("poison") || lower.contains("bad") || lower.contains("error")
                || lower.contains("fail") || lower.contains("crash") || lower.contains("broken");
    }

    private void addEvent(List<Map<String, String>> list, String time, String status, String value, String desc) {
        list.add(0, Map.of("time", time, "status", status, "value", value, "desc", desc));
        while (list.size() > 100) list.remove(list.size() - 1);
    }

    private void addDead(List<Map<String, String>> list, String time, String value) {
        list.add(0, Map.of("time", time, "value", value));
        while (list.size() > 100) list.remove(list.size() - 1);
    }

    private void saveSingleToDb(Map<String, String> dead, String source, String ownerSid) {
        KafkaMessageEntity entity = KafkaMessageEntity.builder()
                .topic("kafka".equals(source) ? "Kafka DLT" : "RabbitMQ DLQ")
                .messageKey("dead-letter")
                .messageValue(dead.get("value"))
                .direction("DLQ")
                .headers("source=" + source + ", original_time=" + dead.getOrDefault("time", ""))
                .groupId("kafka".equals(source) ? "dlq-compare-group" : "dlq-dead-queue")
                .ownerSid(ownerSid)
                .timestamp(LocalDateTime.now())
                .build();
        messageRepository.save(entity);
    }

    private String extractSid(String key) {
        if (key != null && key.contains(SID_SEPARATOR)) {
            return key.substring(0, key.indexOf(SID_SEPARATOR));
        }
        return "_default";
    }

    private String extractSidFromRabbit(String raw) {
        if (raw != null && raw.contains(SID_SEPARATOR)) {
            return raw.substring(0, raw.indexOf(SID_SEPARATOR));
        }
        return "_default";
    }

    private String extractMessageFromRabbit(String raw) {
        if (raw != null && raw.contains(SID_SEPARATOR)) {
            return raw.substring(raw.indexOf(SID_SEPARATOR) + SID_SEPARATOR.length());
        }
        return raw;
    }

    private String now() {
        return LocalDateTime.now().format(FMT);
    }
}
