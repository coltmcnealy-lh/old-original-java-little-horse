package io.littlehorse.common.events;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.objects.rundata.WFRun;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;


public class WFEvent extends BaseSchema {
    public String wfSpecId;
    public String wfSpecName;
    public String wfRunId;
    public Date timestamp;
    public int threadId;

    public WFEventType type;

    // In the case of a Task Run, for example, this is just a serialized TaskRunSchema
    // object. For other things, such as external events, it'll be other things.
    public String content;

    @JsonIgnore
    public WFRun wfRun;

    @JsonIgnore
    public WFSpec wfSpec;

    @JsonIgnore
    public String getKafkaTopic() throws LHConnectionError {
        return config.getWFRunEventTopic();
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
