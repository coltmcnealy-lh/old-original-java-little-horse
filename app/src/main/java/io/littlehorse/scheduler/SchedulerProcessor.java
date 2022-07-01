package io.littlehorse.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.model.metadata.WFSpec;
import io.littlehorse.common.util.Constants;
import io.littlehorse.proto.WFRunEventPb;
import io.littlehorse.proto.WFRunPb;
import io.littlehorse.scheduler.logic.WFRun;


public class SchedulerProcessor implements Processor<
    String, WFRunEventPb, String, SchedulerOutput
> {
    private ProcessorContext<String, SchedulerOutput> ctx;
    private KeyValueStore<String, WFRunPb> store;
    private Map<String, WFSpec> wfSpecCache;
    private LHConfig config;

    public SchedulerProcessor(LHConfig config) {
        this.config = config;
    }

    @Override
    public void init(ProcessorContext<String, SchedulerOutput> ctx) {
        this.ctx = ctx;
        this.store = ctx.getStateStore(Constants.WF_RUN_STORE_NAME);
        wfSpecCache = new HashMap<>();
    }

    @Override
    public void process(Record<String, WFRunEventPb> record) {
        try {
            processHelper(record);
        } catch(Exception exn) {
            exn.printStackTrace();
        }
    }

    private void processHelper(Record<String, WFRunEventPb> record)
    throws LHConnectionError {
        WFRunEventPb evt = record.value();

        WFSpec spec = getWfSpec(evt.getWfSpecId());
        WFRunPb wfRun = store.get(record.key());

        List<SchedulerOutput> sos = WFRun.handleEvent(spec, wfRun, evt);

        for (SchedulerOutput so : sos) {
            Record<String, SchedulerOutput> newRec = new Record<String,SchedulerOutput>(
                record.key(), so, record.timestamp()
            );

            if (so.toSchedule != null) {
                ctx.forward(newRec, SchedulerTopology.taskQueueSink);
            } else if (so.newRun != null) {
                ctx.forward(newRec, SchedulerTopology.wfRunSink);
                store.put(record.key(), so.newRun.build());
            } else {
                // Not possible.
                throw new RuntimeException("WTF?");
            }
        }
    }

    public WFSpec getWfSpec(String wfSpecId) throws LHConnectionError {
        WFSpec out = wfSpecCache.get(wfSpecId);
        if (out == null) {
            out = config.getWFSpec(wfSpecId);
            wfSpecCache.put(wfSpecId, out);
        }
        return out;
    }
}
