package manufacture.ru.brokerlearning.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "kafka_messages")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;

    private Integer partitionNum;

    private Long offsetNum;

    private String messageKey;

    @Column(columnDefinition = "TEXT")
    private String messageValue;

    private String headers;

    private String direction;

    private LocalDateTime timestamp;

    private String groupId;

    private String ownerSid;
}
