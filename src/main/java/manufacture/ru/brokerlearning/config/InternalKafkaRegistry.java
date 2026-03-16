package manufacture.ru.brokerlearning.config;

import java.util.Set;

/**
 * Реестр внутренних топиков и consumer-групп приложения.
 * Используется для скрытия системных ресурсов от пользователя.
 */
public final class InternalKafkaRegistry {

    private InternalKafkaRegistry() {}

    /** Топики, созданные приложением для демонстраций и сервисов */
    public static final Set<String> INTERNAL_TOPICS = Set.of(
            "learning-topic",
            "metrics-topic",
            "orders-topic",
            "benchmark-topic",
            "replay-topic",
            "compare-topic",
            "dlq-compare-topic",
            "dlq-compare-topic.DLT",
            "at-most-once-topic",
            "at-least-once-topic",
            "exactly-once-topic",
            "ordering-1p-topic",
            "ordering-5p-topic",
            "rebalancing-topic",
            "compression-none",
            "compression-gzip",
            "compression-snappy",
            "compression-lz4",
            "compression-zstd"
    );

    /** Consumer-группы, созданные приложением */
    public static final Set<String> INTERNAL_GROUPS = Set.of(
            "broker-learning-group",
            "main-consumer-group",
            "analytics-group",
            "notification-group",
            "batch-group",
            "manual-ack-group",
            "filtered-group",
            "error-handler-group",
            "dlq-compare-group",
            "job-consumer-group",
            "replay-demo-group",
            "rebalancing-demo-group"
    );

    /** Префиксы Kafka-внутренних топиков */
    private static final Set<String> KAFKA_INTERNAL_PREFIXES = Set.of("__");

    public static boolean isInternalTopic(String topic) {
        if (topic == null) return true;
        for (String prefix : KAFKA_INTERNAL_PREFIXES) {
            if (topic.startsWith(prefix)) return true;
        }
        return INTERNAL_TOPICS.contains(topic);
    }

    public static boolean isInternalGroup(String groupId) {
        if (groupId == null) return true;
        return INTERNAL_GROUPS.contains(groupId);
    }

    public static boolean isUserTopic(String topic) {
        return !isInternalTopic(topic);
    }

    public static boolean isUserGroup(String groupId) {
        return !isInternalGroup(groupId);
    }
}
