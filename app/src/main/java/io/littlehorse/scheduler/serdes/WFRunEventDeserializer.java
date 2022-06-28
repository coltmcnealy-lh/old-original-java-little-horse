package io.littlehorse.scheduler.serdes;

import java.util.Date;
import org.apache.kafka.common.serialization.Deserializer;
import com.google.protobuf.InvalidProtocolBufferException;
import io.littlehorse.common.exceptions.VarSubError;
import io.littlehorse.common.model.wfrun.WFRunEvent;
import io.littlehorse.proto.WFRunEventPb;

public class WFRunEventDeserializer implements Deserializer<WFRunEvent> {
    public WFRunEvent deserialize(String topic, byte[] bytes) {
        try {
            long start = System.nanoTime();
            WFRunEventPb proto = WFRunEventPb.parseFrom(bytes);
            WFRunEvent out = WFRunEvent.fromProto(proto);
            System.out.println(System.nanoTime() - start);
            return out;
        } catch(InvalidProtocolBufferException|VarSubError exn) {
            exn.printStackTrace();
            throw new RuntimeException("invalid event: " + exn.getMessage());
        }
    }
}
