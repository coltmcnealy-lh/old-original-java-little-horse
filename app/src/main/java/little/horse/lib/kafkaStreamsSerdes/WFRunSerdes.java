package little.horse.lib.kafkaStreamsSerdes;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.WFRunSchema;


public class WFRunSerdes implements Serde<WFRunSchema> {
    @Override
    public Serializer<WFRunSchema> serializer() {
        return new WFRunSerializer();
    }

    @Override
    public Deserializer<WFRunSchema> deserializer() {
        return new WFRunDeSerializer();
    }
}
