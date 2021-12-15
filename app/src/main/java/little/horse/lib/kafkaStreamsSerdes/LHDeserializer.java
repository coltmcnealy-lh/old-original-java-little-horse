package little.horse.lib.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.schemas.BaseSchema;

public class LHDeserializer<T extends BaseSchema> implements Deserializer<T> {
    @Override
    public T deserialize(String topic, byte[] bytes) {
        return BaseSchema.fromBytes(
            bytes,
            // Just don't show this line to John Hanley
            (Class<? extends BaseSchema>) this.getClass().getGenericSuperclass().getClass()
        );
    }
}
