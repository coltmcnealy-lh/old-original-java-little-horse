package little.horse.lib.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.schemas.WFEventSchema;

public class WFEventSerializer implements Serializer<WFEventSchema> {
    @Override
    public byte[] serialize(String topic, WFEventSchema wfEventThingy) {
        if (wfEventThingy == null) return null;

        try {
            return (new ObjectMapper().writeValueAsString(wfEventThingy)).getBytes();
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
