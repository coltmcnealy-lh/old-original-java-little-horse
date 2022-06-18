package io.littlehorse.common.util.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.BaseSchema;

public class LHDeserializer<T extends BaseSchema> implements Deserializer<T> {
    private Class<T> cls;
    private LHConfig config;

    public LHDeserializer(Class<T> cls, LHConfig config) {
        this.cls = cls;
        this.config = config;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) return null;
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
