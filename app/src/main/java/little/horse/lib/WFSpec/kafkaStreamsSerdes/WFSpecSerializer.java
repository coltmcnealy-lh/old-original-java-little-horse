package little.horse.lib.WFSpec.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecSerializer implements Serializer<WFSpecSchema> {

  @Override
  public byte[] serialize(String topic, WFSpecSchema wfSpecThingy) {
    byte[] result = doSerialize(topic, wfSpecThingy);
    // System.out.println("Got this: ");
    // System.out.println(result);
    // System.out.println(new String(result, StandardCharsets.UTF_8));
    return result;
  }

  public byte[] doSerialize(String topic, WFSpecSchema wfSpecThingy) {
    if (wfSpecThingy == null) return null;
    try {
      return (new ObjectMapper().writeValueAsString(wfSpecThingy)).getBytes();
    } catch(JsonProcessingException exn) {
      return null;
    }
  }
}
