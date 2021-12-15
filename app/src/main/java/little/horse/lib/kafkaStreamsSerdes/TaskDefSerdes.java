package little.horse.lib.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.schemas.TaskDefSchema;


public class TaskDefSerdes implements Serde<TaskDefSchema> {

  @Override
  public Serializer<TaskDefSchema> serializer() {
    return new TaskDefSerializer();
  }

  @Override
  public Deserializer<TaskDefSchema> deserializer() {
    return new TaskDefDeSerializer();
  }
}
