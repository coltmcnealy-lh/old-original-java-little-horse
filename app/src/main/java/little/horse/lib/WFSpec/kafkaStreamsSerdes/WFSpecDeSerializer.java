package little.horse.lib.WFSpec.kafkaStreamsSerdes;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecDeSerializer implements Deserializer<WFSpecSchema> {
  private Gson gson =
      new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

  @Override
  public WFSpecSchema deserialize(String topic, byte[] bytes) {
    if (bytes == null) return null;
    return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), WFSpecSchema.class);
  }
}
