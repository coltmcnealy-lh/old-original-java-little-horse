package io.littlehorse.scheduler.serdes;

import org.apache.kafka.common.serialization.Serializer;
import io.littlehorse.common.model.wfrun.WFRunEvent;

public class WFRunEventSerializer implements Serializer<WFRunEvent> {
    public byte[] serialize(String topic, WFRunEvent event) {
        return event.toProtoBuilder().build().toByteArray();
    }
}
