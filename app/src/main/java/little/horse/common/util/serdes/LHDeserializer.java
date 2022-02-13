package little.horse.common.util.serdes;

import org.apache.kafka.common.serialization.Deserializer;

import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;

public class LHDeserializer<T extends BaseSchema> implements Deserializer<T> {
    private Class<T> cls;
    private Config config;

    public LHDeserializer(Class<T> asdf, Config config) {
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
