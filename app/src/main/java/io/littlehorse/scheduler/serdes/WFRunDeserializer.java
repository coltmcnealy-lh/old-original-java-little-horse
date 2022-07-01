package io.littlehorse.scheduler.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import com.google.protobuf.InvalidProtocolBufferException;
import io.littlehorse.proto.WFRunPb;

public class WFRunDeserializer implements Deserializer<WFRunPb> {
    public WFRunPb deserialize(String topic, byte[] bytes) {
        try {
            return WFRunPb.parseFrom(bytes);
        } catch(InvalidProtocolBufferException exn) {
            exn.printStackTrace();
            throw new RuntimeException(exn);
        }
    }
}
