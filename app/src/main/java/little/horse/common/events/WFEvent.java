package little.horse.common.events;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.LHDatabaseClient;

public class WFEvent extends BaseSchema {
    public String wfSpecDigest;
    public String wfSpecName;
    public String wfRunId;
    public Date timestamp;
    public int threadID;

    public WFEventType type;

    // In the case of a Task Run, for example, this is just a serialized TaskRunSchema
    // object. For other things, such as external events, it'll be other things.
    public String content;

    @JsonIgnore
    public WFRun wfRun;

    @JsonIgnore
    private WFSpec wfSpec;

    @JsonIgnore
    private String kafkaTopic;

    @JsonIgnore
    public String getKafkaTopic() throws LHConnectionError {
        if (kafkaTopic != null) {
            return kafkaTopic;
        }

        if (wfRun != null) {
            return wfRun.getWFSpec().getKafkaTopic();
        }

        if (wfSpec == null) {
            wfSpec = LHDatabaseClient.lookupMeta(
                wfSpecDigest, config, WFSpec.class
            );

            if (wfSpec == null) {
                throw new RuntimeException(
                    "Event has invalid wfSpec id!!"
                );
            }
            kafkaTopic = wfSpec.getKafkaTopic();
            
        }
        return kafkaTopic;
    }

    public void record() throws LHConnectionError {
        if (config == null) {
            throw new RuntimeException(
                "Must set Config for WFEventSchema before recording it!"
            );
        }

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.getKafkaTopic(),
            wfRun.id,
            this.toString()
        );
        config.send(record);
    }
}
