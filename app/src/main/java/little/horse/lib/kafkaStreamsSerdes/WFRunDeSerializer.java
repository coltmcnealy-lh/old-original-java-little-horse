package little.horse.lib.kafkaStreamsSerdes;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import little.horse.lib.WFRunSchema;


public class WFRunDeSerializer implements Deserializer<WFRunSchema> {
    @Override
    public WFRunSchema deserialize(String topic, byte[] bytes) {
        if (bytes == null) return null;

        try {
            WFRunSchema wfRun = new ObjectMapper().readValue(
                bytes,
                WFRunSchema.class
            );
            return wfRun;
        } catch (JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        } catch (IOException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}