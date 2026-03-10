package manufacture.ru.brokerlearning.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicInfo {

    private String name;
    private int partitions;
    private int replicationFactor;
    private Map<String, String> configs;
}
