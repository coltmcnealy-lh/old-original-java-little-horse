package little.horse.api.topology.serdes;

import org.apache.kafka.common.serialization.Deserializer;

import little.horse.common.objects.BaseSchema;

public class LHDeserializer<T extends BaseSchema> implements Deserializer<T> {
    private Class<? extends BaseSchema> cls;
    
    public LHDeserializer(Class<? extends BaseSchema> asdf) {
        this.cls = asdf;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return BaseSchema.fromBytes(
            bytes,
            // Just don't show this line to John Hanley
            cls
            // TaskDefSchema.class
        );
    }
}
