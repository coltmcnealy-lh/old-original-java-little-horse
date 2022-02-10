package little.horse.common.events;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.WFRun;

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

    public void record() throws LHConnectionError {
        if (wfRun == null || config == null) {
            throw new RuntimeException(
                "Must set wfRun and Config for WFEventSchema before recording it!"
            );
        }

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfRun.getWFSpec().kafkaTopic,
            wfRun.id,
            this.toString()
        );
        config.send(record);
    }
}
