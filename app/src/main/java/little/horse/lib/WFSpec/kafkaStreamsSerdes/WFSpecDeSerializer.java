package little.horse.lib.WFSpec.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecDeSerializer implements Deserializer<WFSpecSchema> {

  @Override
  public WFSpecSchema deserialize(String topic, byte[] bytes) {

    if (bytes == null) return null;
    try {
      WFSpecSchema schema = new ObjectMapper().readValue(
        bytes, WFSpecSchema.class
      );
      return schema;
    } catch (JsonProcessingException exn) {
      return null;
    } catch (IOException exn) {
      return null;
    }
  }
}
