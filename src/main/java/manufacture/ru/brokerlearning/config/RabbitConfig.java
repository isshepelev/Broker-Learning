package manufacture.ru.brokerlearning.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    public static final String DEMO_QUEUE = "demo-queue";
    public static final String DEMO_EXCHANGE = "demo-exchange";
    public static final String DEMO_ROUTING_KEY = "demo.message";

    @Bean
    public Queue demoQueue() {
        return new Queue(DEMO_QUEUE, true);
    }

    @Bean
    public TopicExchange demoExchange() {
        return new TopicExchange(DEMO_EXCHANGE);
    }

    @Bean
    public Binding demoBinding(Queue demoQueue, TopicExchange demoExchange) {
        return BindingBuilder.bind(demoQueue).to(demoExchange).with(DEMO_ROUTING_KEY);
    }
}
