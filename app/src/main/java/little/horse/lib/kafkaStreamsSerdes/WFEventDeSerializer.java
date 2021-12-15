package little.horse.lib.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;

import little.horse.lib.schemas.WFEventSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


public class WFEventDeSerializer implements Deserializer<WFEventSchema> {
    @Override
    public WFEventSchema deserialize(String topic, byte[] bytes) {
        if (bytes == null) return null;

        try {
            WFEventSchema wfEvent = new ObjectMapper().readValue(
                bytes,
                WFEventSchema.class
            );
            return wfEvent;
        } catch (JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        } catch (IOException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}