package io.littlehorse.scheduler.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import io.littlehorse.proto.WFRunPb;

public class WFRunSerde implements Serde<WFRunPb> {
    private Serializer<WFRunPb> ser;
    private Deserializer<WFRunPb> deser;
    
    public WFRunSerde() {
        this.ser = new WFRunSerializer();
        this.deser = new WFRunDeserializer();
    }

    public Serializer<WFRunPb> serializer() {
        return this.ser;
    }

    public Deserializer<WFRunPb> deserializer() {
        return this.deser;
    }
}
