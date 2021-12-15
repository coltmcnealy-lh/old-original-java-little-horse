package little.horse.lib.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.schemas.BaseSchema;


public class LHSerdes<T extends BaseSchema> implements Serde<T> {
    @Override
    public Serializer<T> serializer() {
        return new LHSerializer<T>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new LHDeserializer<T>();
    }
}
