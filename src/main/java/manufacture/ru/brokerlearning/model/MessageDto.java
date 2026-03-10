package manufacture.ru.brokerlearning.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageDto {

    private String topic;
    private String key;
    private String value;
    private Integer partition;
    private Map<String, String> headers;
    private String sendMode;
}
