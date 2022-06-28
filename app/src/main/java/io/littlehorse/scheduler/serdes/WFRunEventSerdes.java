package io.littlehorse.scheduler.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import io.littlehorse.common.model.wfrun.WFRunEvent;

public class WFRunEventSerdes implements Serde<WFRunEvent> {
    WFRunEventDeserializer deser;
    WFRunEventSerializer ser;
    
    public WFRunEventSerdes() {
        this.deser = new WFRunEventDeserializer();
        this.ser = new WFRunEventSerializer();
    }

    public Serializer<WFRunEvent> serializer() {
        return ser;
    }

    public Deserializer<WFRunEvent> deserializer() {
        return deser;
    }
}
