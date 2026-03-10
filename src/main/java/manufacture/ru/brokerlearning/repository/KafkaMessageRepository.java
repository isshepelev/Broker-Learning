package manufacture.ru.brokerlearning.repository;

import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface KafkaMessageRepository extends JpaRepository<KafkaMessageEntity, Long> {

    List<KafkaMessageEntity> findByTopicOrderByTimestampDesc(String topic);

    List<KafkaMessageEntity> findByDirectionOrderByTimestampDesc(String direction);

    List<KafkaMessageEntity> findTop100ByOrderByTimestampDesc();
}
