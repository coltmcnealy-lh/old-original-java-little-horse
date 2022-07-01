package io.littlehorse.scheduler.serdes;

import org.apache.kafka.common.serialization.Serializer;
import io.littlehorse.proto.WFRunPb;

public class WFRunSerializer implements Serializer<WFRunPb> {
    public byte[] serialize(String topic, WFRunPb proto) {
        return proto.toByteArray();
    }
}
