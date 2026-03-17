package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/compression")
@Slf4j
@RequiredArgsConstructor
public class CompressionController {

    private static final String TOPIC_PREFIX = "compression-";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final UserSessionHelper sessionHelper;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "compression");
        return "compression";
    }

    @PostMapping("/run")
    @ResponseBody
    public Map<String, Object> run(@RequestBody Map<String, Object> request) {
        int count = request.get("count") != null ? ((Number) request.get("count")).intValue() : 1000;
        count = Math.max(10, Math.min(count, 50000));

        // Генерируем реалистичные JSON-сообщения для наглядности сжатия
        String sampleMessage = generateJsonMessage();
        int msgSizeBytes = sampleMessage.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();

        String[] codecs = {"none", "gzip", "snappy", "lz4", "zstd"};
        String[] labels = {"Без сжатия", "GZIP", "Snappy", "LZ4", "ZSTD"};
        String[] colors = {"#6b7280", "#ef4444", "#f59e0b", "#3b82f6", "#8b5cf6"};
        String[] descriptions = {
                "Никакого сжатия. Максимальный размер, минимальная нагрузка на CPU.",
                "Лучшее сжатие, но самый медленный. Подходит для редких больших сообщений.",
                "Быстрое сжатие от Google. Хороший баланс скорости и размера.",
                "Самое быстрое сжатие. Минимальная задержка, умеренное сжатие.",
                "Facebook Zstandard. Лучший баланс скорости и степени сжатия."
        };

        try {
            for (int i = 0; i < codecs.length; i++) {
                results.add(benchCompression(codecs[i], labels[i], colors[i], descriptions[i], count, sampleMessage));
            }

            response.put("success", true);
            response.put("results", results);
            response.put("count", count);
            response.put("msgSizeBytes", msgSizeBytes);
        } catch (Exception e) {
            log.error("Compression benchmark failed: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return response;
    }

    private Map<String, Object> benchCompression(String codec, String label, String color,
                                                  String description, int count, String message) throws Exception {
        String sid = sessionHelper.currentSid();
        String topic = TOPIC_PREFIX + sid + "-" + codec;

        // Создаём топик
        ensureTopic(topic);

        // Создаём producer с указанным сжатием
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        // Отправляем
        var futures = new ArrayList<java.util.concurrent.CompletableFuture<?>>();
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            futures.add(template.send(topic, "k-" + i, message));
        }
        for (var f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        long elapsed = System.nanoTime() - start;
        double timeMs = elapsed / 1_000_000.0;

        factory.destroy();

        // Читаем размер топика из брокера
        long topicSizeBytes = measureTopicSize(topic);

        long rawSize = (long) message.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * count;
        double ratio = topicSizeBytes > 0 ? (double) rawSize / topicSizeBytes : 1.0;
        double savedPercent = topicSizeBytes > 0 && topicSizeBytes < rawSize
                ? (1.0 - (double) topicSizeBytes / rawSize) * 100.0 : 0.0;

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("codec", codec);
        r.put("label", label);
        r.put("color", color);
        r.put("description", description);
        r.put("timeMs", Math.round(timeMs * 100.0) / 100.0);
        r.put("msgPerSec", count > 0 ? Math.round(count / (timeMs / 1000.0)) : 0);
        r.put("rawSizeBytes", rawSize);
        r.put("topicSizeBytes", topicSizeBytes);
        r.put("ratio", Math.round(ratio * 100.0) / 100.0);
        r.put("savedPercent", Math.round(savedPercent * 10.0) / 10.0);
        r.put("count", count);

        log.info("Compression [{}]: {} msgs, time={}ms, raw={}b, topic={}b, ratio={}x",
                codec, count, Math.round(timeMs), rawSize, topicSizeBytes, Math.round(ratio * 10.0) / 10.0);
        return r;
    }

    /**
     * Измеряет реальный размер топика на диске через AdminClient.describeLogDirs().
     * Consumer распаковывает данные, поэтому через него нельзя узнать сжатый размер.
     */
    private long measureTopicSize(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            // Получаем ID всех брокеров
            var nodes = admin.describeCluster().nodes().get();
            List<Integer> brokerIds = nodes.stream().map(n -> n.id()).collect(Collectors.toList());

            // Запрашиваем информацию о log dirs
            var logDirs = admin.describeLogDirs(brokerIds).allDescriptions().get();

            long totalSize = 0;
            for (var entry : logDirs.entrySet()) {
                for (var logDir : entry.getValue().values()) {
                    for (var replicaEntry : logDir.replicaInfos().entrySet()) {
                        if (replicaEntry.getKey().topic().equals(topic)) {
                            totalSize += replicaEntry.getValue().size();
                        }
                    }
                }
            }
            return totalSize;
        } catch (Exception e) {
            log.warn("measureTopicSize error for {}: {}", topic, e.getMessage());
            return 0;
        }
    }

    private void ensureTopic(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(topic)) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                Thread.sleep(300);
            } else {
                // Очищаем существующий топик через deleteRecords
                TopicPartition tp = new TopicPartition(topic, 0);
                Map<TopicPartition, Long> endOffsets;
                try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
                    c.assign(List.of(tp));
                    endOffsets = c.endOffsets(List.of(tp));
                }
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                if (endOffset > 0) {
                    admin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(endOffset))).all().get();
                }
            }
        } catch (Exception e) {
            log.warn("ensureTopic error: {}", e.getMessage());
        }
    }

    private String generateJsonMessage() {
        return "{\"userId\":\"usr-12345\",\"event\":\"page_view\",\"timestamp\":\"2026-03-10T12:00:00Z\"," +
                "\"properties\":{\"url\":\"/products/kafka-learning\",\"referrer\":\"https://google.com\"," +
                "\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36\"," +
                "\"ip\":\"192.168.1.100\",\"sessionId\":\"sess-abcdef-123456\",\"duration\":4500," +
                "\"tags\":[\"kafka\",\"streaming\",\"real-time\",\"messaging\",\"distributed\"]," +
                "\"metadata\":{\"region\":\"eu-west-1\",\"version\":\"2.1.0\",\"environment\":\"production\"}}}";
    }
}
