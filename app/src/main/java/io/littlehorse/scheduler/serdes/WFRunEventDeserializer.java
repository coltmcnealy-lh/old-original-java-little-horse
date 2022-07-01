package io.littlehorse.scheduler.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import com.google.protobuf.InvalidProtocolBufferException;
import io.littlehorse.proto.WFRunEventPb;

public class WFRunEventDeserializer implements Deserializer<WFRunEventPb> {
    public WFRunEventPb deserialize(String topic, byte[] bytes) {
        try {
            long start = System.nanoTime();
            WFRunEventPb proto = WFRunEventPb.parseFrom(bytes);
            // WFRunEvent out = WFRunEvent.fromProto(proto);
            System.out.println(System.nanoTime() - start);
            return proto;
            // return out;
        } catch(InvalidProtocolBufferException exn) {
            exn.printStackTrace();
            throw new RuntimeException(exn);
        }
    }
}
