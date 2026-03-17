package manufacture.ru.brokerlearning.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Трекинг принадлежности Kafka-ресурсов (топики, consumer group) пользователям.
 */
@Entity
@Table(name = "user_resources", uniqueConstraints = @UniqueConstraint(columnNames = {"resourceType", "resourceName"}))
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserResource {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Тип ресурса: TOPIC или CONSUMER_GROUP */
    @Column(nullable = false)
    private String resourceType;

    /** Имя ресурса (topic name или group id) */
    @Column(nullable = false)
    private String resourceName;

    /** sid владельца */
    @Column(nullable = false)
    private String ownerSid;

    private LocalDateTime createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = LocalDateTime.now();
    }
}
