package little.horse.common.util.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.common.DepInjContext;
import little.horse.common.objects.BaseSchema;


public class LHSerdes<T extends BaseSchema> implements Serde<T> {
    private Class<T> cls;
    private DepInjContext config;
    
    public LHSerdes(Class<T> asdf, DepInjContext config) {
        this.cls = asdf;
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
