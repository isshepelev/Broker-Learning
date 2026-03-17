package manufacture.ru.brokerlearning.repository;

import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface KafkaMessageRepository extends JpaRepository<KafkaMessageEntity, Long> {

    List<KafkaMessageEntity> findByTopicOrderByTimestampDesc(String topic);

    List<KafkaMessageEntity> findByDirectionOrderByTimestampDesc(String direction);

    List<KafkaMessageEntity> findTop100ByOrderByTimestampDesc();

    long countByDirection(String direction);

    long countByTimestampAfter(LocalDateTime after);

    List<KafkaMessageEntity> findByTimestampAfterOrderByTimestampAsc(LocalDateTime after);

    List<KafkaMessageEntity> findByDirectionAndTimestampAfterOrderByTimestampDesc(String direction, LocalDateTime after);

    @Query("SELECT m.topic, COUNT(m) FROM KafkaMessageEntity m WHERE m.direction = :direction GROUP BY m.topic")
    List<Object[]> countByTopicAndDirection(@Param("direction") String direction);

    // Per-user queries
    List<KafkaMessageEntity> findTop100ByOwnerSidOrderByTimestampDesc(String ownerSid);

    long countByDirectionAndOwnerSid(String direction, String ownerSid);

    long countByOwnerSidAndTimestampAfter(String ownerSid, LocalDateTime after);

    List<KafkaMessageEntity> findByOwnerSidAndTimestampAfterOrderByTimestampAsc(String ownerSid, LocalDateTime after);

    List<KafkaMessageEntity> findByDirectionAndOwnerSidOrderByTimestampDesc(String direction, String ownerSid);

    List<KafkaMessageEntity> findByOwnerSidAndDirectionAndTimestampAfterOrderByTimestampDesc(
            String ownerSid, String direction, LocalDateTime after);
}
