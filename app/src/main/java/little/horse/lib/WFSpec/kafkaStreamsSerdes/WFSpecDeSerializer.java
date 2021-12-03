package little.horse.lib.WFSpec.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecDeSerializer implements Deserializer<WFSpecSchema> {
  private Gson gson =
      new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

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
