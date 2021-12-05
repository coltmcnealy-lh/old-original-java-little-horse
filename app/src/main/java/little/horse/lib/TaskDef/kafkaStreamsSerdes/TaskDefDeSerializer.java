package little.horse.lib.TaskDef.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.TaskDef.TaskDefSchema;

public class TaskDefDeSerializer implements Deserializer<TaskDefSchema> {

  @Override
  public TaskDefSchema deserialize(String topic, byte[] bytes) {

    if (bytes == null) return null;
    try {
      TaskDefSchema schema = new ObjectMapper().readValue(
        bytes, TaskDefSchema.class
      );
      return schema;
    } catch (JsonProcessingException exn) {
      return null;
    } catch (IOException exn) {
      return null;
    }
  }
}
