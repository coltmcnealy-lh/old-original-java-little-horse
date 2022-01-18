package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.streams.processor.api.Record;

import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHStatus;
import little.horse.lib.wfRuntime.WFRunStatus;

public class WFSpecSchema extends BaseSchema {
    public String name;
    public String guid;
    public LHStatus status;
    public String kafkaTopic;
    public String entrypointThreadName;
    public LHStatus desiredStatus;

    public HashMap<String, ThreadSpecSchema> threadSpecs;

    // All fields below ignored by Json
    @JsonIgnore
    private WFSpecSchema wfSpec;

    @JsonIgnore
    public ArrayList<Map.Entry<String, NodeSchema>> allNodePairs() {
        ArrayList<Map.Entry<String, NodeSchema>> out = new ArrayList<>();
        for (Map.Entry<String, ThreadSpecSchema> tp: threadSpecs.entrySet()) {
            ThreadSpecSchema t = tp.getValue();
            for (Map.Entry<String, NodeSchema> np: t.nodes.entrySet()) {
                out.add(np);
            }
        }
        return out;
    }

    @JsonIgnore
    public WFRunSchema newRun(
        final Record<String, WFEventSchema> record
    ) throws LHNoConfigException, LHLookupException {
        WFRunSchema wfRun = new WFRunSchema();
        WFEventSchema event = record.value();
        WFRunRequestSchema runRequest = BaseSchema.fromString(
            event.content, WFRunRequestSchema.class
        );

        wfRun.guid = record.key();
        wfRun.wfSpecGuid = event.wfSpecGuid;
        wfRun.wfSpecName = event.wfSpecName;
        wfRun.setWFSpec(this);

        wfRun.status = WFRunStatus.RUNNING;
        wfRun.threadRuns = new ArrayList<ThreadRunSchema>();
        wfRun.correlatedEvents =
            new HashMap<String, ArrayList<ExternalEventCorrelSchema>>();

        wfRun.startTime = event.timestamp;
        wfRun.awaitableThreads = new HashMap<String, ArrayList<ThreadRunMetaSchema>>();

        wfRun.addThread(
            entrypointThreadName, runRequest.variables, WFRunStatus.RUNNING
        );

        return wfRun;
    }


}
