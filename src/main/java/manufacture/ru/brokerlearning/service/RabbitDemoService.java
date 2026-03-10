package manufacture.ru.brokerlearning.service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.RabbitConfig;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Демо-сервис RabbitMQ для сравнения с Kafka.
 * Показывает модель "классическая очередь": сообщение удаляется после прочтения.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitDemoService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final RabbitTemplate rabbitTemplate;

    @Getter
    private final List<Map<String, String>> sentMessages = Collections.synchronizedList(new ArrayList<>());
    @Getter
    private final List<Map<String, String>> receivedMessages = Collections.synchronizedList(new ArrayList<>());

    private final AtomicInteger sentCount = new AtomicInteger(0);
    private final AtomicInteger receivedCount = new AtomicInteger(0);

    public Map<String, String> send(String message) {
        String time = LocalDateTime.now().format(FMT);
        rabbitTemplate.convertAndSend(
                RabbitConfig.DEMO_EXCHANGE,
                RabbitConfig.DEMO_ROUTING_KEY,
                message
        );
        sentCount.incrementAndGet();
        Map<String, String> entry = Map.of("time", time, "value", message);
        sentMessages.add(0, entry);
        trimList(sentMessages, 50);
        log.info("RabbitMQ SENT: {}", message);
        return entry;
    }

    @RabbitListener(queues = RabbitConfig.DEMO_QUEUE)
    public void receive(String message) {
        String time = LocalDateTime.now().format(FMT);
        receivedCount.incrementAndGet();
        receivedMessages.add(0, Map.of("time", time, "value", message));
        trimList(receivedMessages, 50);
        log.info("RabbitMQ RECEIVED: {} (message is now REMOVED from queue)", message);
    }

    public int getSentCount() {
        return sentCount.get();
    }

    public int getReceivedCount() {
        return receivedCount.get();
    }

    public void clear() {
        sentMessages.clear();
        receivedMessages.clear();
        sentCount.set(0);
        receivedCount.set(0);
    }

    private void trimList(List<?> list, int max) {
        while (list.size() > max) {
            list.remove(list.size() - 1);
        }
    }
}
