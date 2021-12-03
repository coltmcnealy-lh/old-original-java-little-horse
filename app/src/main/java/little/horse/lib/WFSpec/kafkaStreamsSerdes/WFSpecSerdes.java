package little.horse.lib.WFSpec.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecSerdes implements Serde<WFSpecSchema> {

  @Override
  public Serializer<WFSpecSchema> serializer() {
    return new WFSpecSerializer();
  }

  @Override
  public Deserializer<WFSpecSchema> deserializer() {
    return new WFSpecDeSerializer();
  }
}
