package manufacture.ru.brokerlearning.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.TopicInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

import manufacture.ru.brokerlearning.config.InternalKafkaRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaAdminService {

    private final AdminClient adminClient;

    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        log.info("Listing all Kafka topics");
        Set<String> topics = adminClient.listTopics().names().get().stream()
                .filter(InternalKafkaRegistry::isUserTopic)
                .collect(Collectors.toSet());
        log.debug("Found {} user topics", topics.size());
        return topics;
    }

    public TopicInfo getTopicInfo(String name) throws ExecutionException, InterruptedException {
        log.info("Getting info for topic: {}", name);

        TopicDescription description = adminClient.describeTopics(Collections.singletonList(name))
                .topicNameValues()
                .get(name)
                .get();

        Map<String, String> configs = getTopicConfigs(name);

        return TopicInfo.builder()
                .name(description.name())
                .partitions(description.partitions().size())
                .replicationFactor(description.partitions().get(0).replicas().size())
                .configs(configs)
                .build();
    }

    public void createTopic(String name, int partitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        log.info("Creating topic: {} with {} partitions and replication factor {}",
                name, partitions, replicationFactor);

        NewTopic newTopic = new NewTopic(name, partitions, replicationFactor);
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

        log.info("Topic '{}' created successfully", name);
    }

    public void deleteTopic(String name) throws ExecutionException, InterruptedException {
        if (InternalKafkaRegistry.isInternalTopic(name)) {
            throw new IllegalArgumentException("Нельзя удалить системный топик: " + name);
        }
        log.info("Deleting topic: {}", name);
        adminClient.deleteTopics(Collections.singletonList(name)).all().get();
        log.info("Topic '{}' deleted successfully", name);
    }

    public Map<String, String> getTopicConfigs(String name) throws ExecutionException, InterruptedException {
        log.info("Getting configs for topic: {}", name);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);

        return adminClient.describeConfigs(Collections.singletonList(resource))
                .all()
                .get()
                .get(resource)
                .entries()
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.name(),
                        entry -> entry.value() != null ? entry.value() : ""
                ));
    }
}
