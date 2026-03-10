package manufacture.ru.brokerlearning.serializer;

import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class CustomJsonSerializer implements Serializer<Object> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            log.debug("Null data received for serialization on topic: {}", topic);
            return null;
        }

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(data);
            log.debug("Serialized message for topic: {}, size: {} bytes", topic, bytes.length);
            return bytes;
        } catch (Exception e) {
            log.error("Error serializing object for topic: {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Error serializing object to JSON", e);
        }
    }

    @Override
    public void close() {
        // No resources to release
    }
}
