package little.horse.lib.schemas;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.WFEventType;

public class WFEventSchema extends BaseSchema {
    public String wfSpecGuid;
    public String wfSpecName;
    public String wfRunGuid;
    public Date timestamp;
    public int threadID;

    public WFEventType type;

    // In the case of a Task Run, for example, this is just a serialized TaskRunSchema
    // object. For other things, such as external events, it'll be other things.
    public String content;

    @JsonIgnore
    public WFRunSchema wfRun;

    public void record() throws LHNoConfigException, LHLookupException {
        if (wfRun == null || config == null) {
            throw new LHNoConfigException(
                "Must set wfRun and Config for WFEventSchema before recording it!"
            );
        }

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfRun.getWFSpec().kafkaTopic,
            wfRun.guid,
            this.toString()
        );
        config.send(record);
    }
}
