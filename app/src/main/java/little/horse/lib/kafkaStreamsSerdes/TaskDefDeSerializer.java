package little.horse.lib.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.TaskDefSchema;

public class TaskDefDeSerializer implements Deserializer<TaskDefSchema> {

  @Override
  public TaskDefSchema deserialize(String topic, byte[] bytes) {

    if (bytes == null) return null;
    return BaseSchema.fromBytes(bytes, TaskDefSchema.class);
  }
}
