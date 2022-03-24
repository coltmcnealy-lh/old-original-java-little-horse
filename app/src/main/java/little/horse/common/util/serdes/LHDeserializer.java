package little.horse.common.util.serdes;

import org.apache.kafka.common.serialization.Deserializer;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;

public class LHDeserializer<T extends BaseSchema> implements Deserializer<T> {
    private Class<T> cls;
    private DepInjContext config;

    public LHDeserializer(Class<T> asdf, DepInjContext config) {
        this.cls = asdf;
        this.config = config;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        try {
            return BaseSchema.fromBytes(
                bytes,
                cls,
                config
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
