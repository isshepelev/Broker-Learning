package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Controller
@RequestMapping("/benchmark")
@Slf4j
@RequiredArgsConstructor
public class BenchmarkController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final UserSessionHelper sessionHelper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "benchmark");
        return "benchmark";
    }

    @PostMapping("/run")
    @ResponseBody
    public Map<String, Object> runBenchmark(@RequestBody Map<String, Object> request) {
        int count = request.get("count") != null ? ((Number) request.get("count")).intValue() : 1000;
        count = Math.min(count, 10000);

        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();
        String sid = sessionHelper.currentSid();
        String topic = "benchmark-topic-" + sid;

        try {
            // 1. Fire-and-Forget
            results.add(benchFireAndForget(topic, count));

            // 2. Sync
            results.add(benchSync(topic, count));

            // 3. Callback (async с ожиданием всех)
            results.add(benchCallback(topic, count));

            // 4. Transactional
            results.add(benchTransactional(topic, count));

            response.put("success", true);
            response.put("results", results);
            response.put("count", count);
        } catch (Exception e) {
            log.error("Benchmark failed: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return response;
    }

    private Map<String, Object> benchFireAndForget(String topic, int count) {
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            kafkaTemplate.send(topic, "ff-" + i, "fire-and-forget-message-" + i);
        }
        long elapsed = System.nanoTime() - start;
        double ms = elapsed / 1_000_000.0;

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("mode", "Fire-and-Forget");
        r.put("description", "Отправка без ожидания подтверждения. Максимальная скорость, возможна потеря.");
        r.put("timeMs", Math.round(ms * 100.0) / 100.0);
        r.put("msgPerSec", count > 0 ? Math.round(count / (ms / 1000.0)) : 0);
        r.put("count", count);
        r.put("color", "#0d9488");
        log.info("Fire-and-forget: {} msgs in {} ms", count, Math.round(ms));
        return r;
    }

    private Map<String, Object> benchSync(String topic, int count) throws Exception {
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            kafkaTemplate.send(topic, "sync-" + i, "sync-message-" + i).get(10, TimeUnit.SECONDS);
        }
        long elapsed = System.nanoTime() - start;
        double ms = elapsed / 1_000_000.0;

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("mode", "Sync");
        r.put("description", "Ожидание подтверждения каждого сообщения. Надёжно, но медленно.");
        r.put("timeMs", Math.round(ms * 100.0) / 100.0);
        r.put("msgPerSec", count > 0 ? Math.round(count / (ms / 1000.0)) : 0);
        r.put("count", count);
        r.put("color", "#3b82f6");
        log.info("Sync: {} msgs in {} ms", count, Math.round(ms));
        return r;
    }

    private Map<String, Object> benchCallback(String topic, int count) throws Exception {
        var futures = new ArrayList<java.util.concurrent.CompletableFuture<?>>();
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            futures.add(kafkaTemplate.send(topic, "cb-" + i, "callback-message-" + i));
        }
        // Ждём завершения всех
        for (var f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        long elapsed = System.nanoTime() - start;
        double ms = elapsed / 1_000_000.0;

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("mode", "Async (Callback)");
        r.put("description", "Асинхронная отправка, ожидание всех подтверждений в конце. Баланс скорости и надёжности.");
        r.put("timeMs", Math.round(ms * 100.0) / 100.0);
        r.put("msgPerSec", count > 0 ? Math.round(count / (ms / 1000.0)) : 0);
        r.put("count", count);
        r.put("color", "#f59e0b");
        log.info("Callback: {} msgs in {} ms", count, Math.round(ms));
        return r;
    }

    private Map<String, Object> benchTransactional(String topic, int count) {
        // Создаём отдельный transactional template для бенчмарка
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        String txPrefix = "bench-tx-" + sessionHelper.currentSid() + "-";
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txPrefix);
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix(txPrefix);
        KafkaTemplate<String, String> txTemplate = new KafkaTemplate<>(factory);

        long start = System.nanoTime();
        txTemplate.executeInTransaction(ops -> {
            for (int i = 0; i < count; i++) {
                ops.send(topic, "tx-" + i, "transactional-message-" + i);
            }
            return null;
        });
        long elapsed = System.nanoTime() - start;
        double ms = elapsed / 1_000_000.0;

        factory.destroy();

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("mode", "Transactional");
        r.put("description", "Все сообщения в одной транзакции. Exactly-once гарантия, дополнительные накладные расходы.");
        r.put("timeMs", Math.round(ms * 100.0) / 100.0);
        r.put("msgPerSec", count > 0 ? Math.round(count / (ms / 1000.0)) : 0);
        r.put("count", count);
        r.put("color", "#a855f7");
        log.info("Transactional: {} msgs in {} ms", count, Math.round(ms));
        return r;
    }
}
