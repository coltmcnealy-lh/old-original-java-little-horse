package little.horse.api.topology.serdes;

import org.apache.kafka.common.serialization.Serializer;

import little.horse.common.objects.BaseSchema;

public class LHSerializer<T extends BaseSchema> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T thingy) {
        if (thingy == null) return null;

        String str = thingy.toString();

        if (str == null) return null;

        return str.getBytes();
    }
}
