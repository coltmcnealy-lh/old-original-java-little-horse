package io.littlehorse.common.util.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.objects.BaseSchema;


public class LHSerdes<T extends BaseSchema> implements Serde<T> {
    private Class<T> cls;
    private DepInjContext config;
    
    public LHSerdes(Class<T> cls, DepInjContext config) {
        this.cls = cls;
        this.config = config;
    }

    @Override
    public Serializer<T> serializer() {
        return new LHSerializer<T>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new LHDeserializer<T>(cls, config);
    }
}
