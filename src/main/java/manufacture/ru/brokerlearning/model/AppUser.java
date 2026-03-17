package manufacture.ru.brokerlearning.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "app_users")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppUser {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String username;

    @Column(nullable = false)
    private String password;

    /** Стабильный короткий ID для именования топиков/групп. У админа sid = "admin" → топики без суффикса */
    @Column(unique = true, nullable = false, length = 20)
    private String sid;

    @Column(nullable = false)
    @lombok.Builder.Default
    private String role = "ROLE_USER";

    private LocalDateTime createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = LocalDateTime.now();
    }
}
