package manufacture.ru.brokerlearning.config;

import java.util.Set;

public final class InternalKafkaRegistry {

    private InternalKafkaRegistry() {}

    /** Топики, созданные приложением (глобальные, не per-user) */
    public static final Set<String> INTERNAL_TOPICS = Set.of(
            "learning-topic",
            "orders-topic",
            "dlq-compare-topic",
            "dlq-compare-topic.DLT"
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
            "dlq-compare-group"
    );

    /** Префиксы per-user и системных топиков/групп */
    private static final Set<String> KAFKA_INTERNAL_PREFIXES = Set.of(
            "__",
            "replay-topic-", "replay-group-",
            "_practice-status-", "_topic-detail-reader-",
            "rebalancing-topic-", "rebalancing-group-",
            "ordering-1p-", "ordering-5p-",
            "compare-topic-",
            "compression-",
            "metrics-topic-", "job-consumer-group-",
            "at-most-once-", "at-least-once-", "exactly-once-",
            "benchmark-topic-"
    );

    public static boolean isInternalTopic(String topic) {
        if (topic == null) return true;
        for (String prefix : KAFKA_INTERNAL_PREFIXES) {
            if (topic.startsWith(prefix)) return true;
        }
        return INTERNAL_TOPICS.contains(topic);
    }

    public static boolean isInternalGroup(String groupId) {
        if (groupId == null) return true;
        for (String prefix : KAFKA_INTERNAL_PREFIXES) {
            if (groupId.startsWith(prefix)) return true;
        }
        return INTERNAL_GROUPS.contains(groupId);
    }

    public static boolean isUserTopic(String topic) {
        return !isInternalTopic(topic);
    }

    public static boolean isUserGroup(String groupId) {
        return !isInternalGroup(groupId);
    }
}
