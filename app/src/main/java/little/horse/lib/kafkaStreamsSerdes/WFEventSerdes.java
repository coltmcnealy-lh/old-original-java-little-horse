package little.horse.lib.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.WFEventSchema;


public class WFEventSerdes implements Serde<WFEventSchema> {

  @Override
  public Serializer<WFEventSchema> serializer() {
    return new WFEventSerializer();
  }

  @Override
  public Deserializer<WFEventSchema> deserializer() {
    return new WFEventDeSerializer();
  }
}
