package manufacture.ru.brokerlearning.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic learningTopic() {
        return new NewTopic("learning-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic metricsTopic() {
        return new NewTopic("metrics-topic", 1, (short) 1);
    }

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic("orders-topic", 3, (short) 1);
    }
}
