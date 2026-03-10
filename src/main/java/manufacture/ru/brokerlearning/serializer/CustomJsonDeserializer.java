package manufacture.ru.brokerlearning.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class CustomJsonDeserializer implements Deserializer<String> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            log.debug("Null data received for deserialization on topic: {}", topic);
            return null;
        }

        try {
            String value = new String(data, StandardCharsets.UTF_8);
            log.debug("Deserialized message from topic: {}, length: {} chars", topic, value.length());
            return value;
        } catch (Exception e) {
            log.error("Error deserializing data from topic: {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Error deserializing data to String", e);
        }
    }

    @Override
    public void close() {
        // No resources to release
    }
}
