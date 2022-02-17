package little.horse.common.events;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;

import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.LHDatabaseClient;

public class WFEvent extends BaseSchema {
    public String wfSpecId;
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
    public WFSpec wfSpec;

    @JsonIgnore
    private String kafkaTopic;

    @JsonIgnore
    public String getKafkaTopic() throws LHConnectionError {
        if (kafkaTopic != null) {
            return kafkaTopic;
        }

        if (wfRun != null) {
            return wfRun.getWFSpec().getEventTopic();
        }

        if (wfSpec == null) {
            wfSpec = LHDatabaseClient.lookupMetaNameOrId(
                wfSpecId, config, WFSpec.class
            );

            if (wfSpec == null) {
                throw new RuntimeException(
                    "Event has invalid wfSpec id!!"
                );
            }
        }
        kafkaTopic = wfSpec.getEventTopic();
        return kafkaTopic;
    }

    public void record() throws LHConnectionError {
        if (config == null) {
            throw new RuntimeException(
                "Must set Config for WFEventSchema before recording it!"
            );
        }

        ProducerRecord<String, Bytes> record = new ProducerRecord<String, Bytes>(
            this.getKafkaTopic(),
            wfRunId,
            new Bytes(this.toBytes())
        );
        config.send(record);
    }
}
