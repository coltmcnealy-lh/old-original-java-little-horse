package little.horse.lib.WFSpec.kafkaStreamsSerdes;

import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecSerializer implements Serializer<WFSpecSchema> {
  private Gson gson = new Gson();

  @Override
  public byte[] serialize(String topic, WFSpecSchema wfSpecThingy) {
    if (wfSpecThingy == null) return null;
    return gson.toJson(wfSpecThingy).getBytes(StandardCharsets.UTF_8);
  }
}
