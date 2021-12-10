package little.horse.lib.kafkaStreamsSerdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import little.horse.lib.WFRunSchema;

public class WFRunSerializer implements Serializer<WFRunSchema> {
    @Override
    public byte[] serialize(String topic, WFRunSchema wfRunThingy) {
        if (wfRunThingy == null) return null;

        try {
            return (new ObjectMapper().writeValueAsString(wfRunThingy)).getBytes();
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
