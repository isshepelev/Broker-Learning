package manufacture.ru.brokerlearning.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    // --- Demo queue (for compare page) ---
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

    // --- DLQ demo ---
    public static final String DLQ_WORK_QUEUE = "dlq-work-queue";
    public static final String DLQ_WORK_EXCHANGE = "dlq-work-exchange";
    public static final String DLQ_WORK_KEY = "dlq.work";

    public static final String DLQ_DEAD_QUEUE = "dlq-dead-queue";
    public static final String DLQ_DEAD_EXCHANGE = "dlq-dead-exchange";

    // Dead Letter Exchange
    @Bean
    public FanoutExchange dlqDeadExchange() {
        return new FanoutExchange(DLQ_DEAD_EXCHANGE);
    }

    // Dead Letter Queue
    @Bean
    public Queue dlqDeadQueue() {
        return new Queue(DLQ_DEAD_QUEUE, true);
    }

    @Bean
    public Binding dlqDeadBinding(Queue dlqDeadQueue, FanoutExchange dlqDeadExchange) {
        return BindingBuilder.bind(dlqDeadQueue).to(dlqDeadExchange);
    }

    // Work queue with DLX configured
    @Bean
    public TopicExchange dlqWorkExchange() {
        return new TopicExchange(DLQ_WORK_EXCHANGE);
    }

    @Bean
    public Queue dlqWorkQueue() {
        return QueueBuilder.durable(DLQ_WORK_QUEUE)
                .withArgument("x-dead-letter-exchange", DLQ_DEAD_EXCHANGE)
                .build();
    }

    @Bean
    public Binding dlqWorkBinding(Queue dlqWorkQueue, TopicExchange dlqWorkExchange) {
        return BindingBuilder.bind(dlqWorkQueue).to(dlqWorkExchange).with(DLQ_WORK_KEY);
    }
}
