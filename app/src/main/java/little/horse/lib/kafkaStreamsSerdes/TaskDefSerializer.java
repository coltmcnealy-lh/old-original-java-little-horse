package little.horse.lib.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.TaskDefSchema;

public class TaskDefSerializer implements Serializer<TaskDefSchema> {

  @Override
  public byte[] serialize(String topic, TaskDefSchema wfSpecThingy) {
    if (wfSpecThingy == null) return null;
    try {
      return (new ObjectMapper().writeValueAsString(wfSpecThingy)).getBytes();
    } catch(JsonProcessingException exn) {
      return null;
    }
  }
}
