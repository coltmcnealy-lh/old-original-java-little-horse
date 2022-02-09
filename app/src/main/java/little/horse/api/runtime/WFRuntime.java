package little.horse.api.runtime;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.Config;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFEventType;
import little.horse.common.events.WFRunRequestSchema;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.ThreadRun;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;


public class WFRuntime
    implements Processor<String, WFEventSchema, String, WFRun>
{
    private KeyValueStore<String, WFRun> wfRunStore;
    private KeyValueStore<String, WFSpec> wfSpecStore;
    private Config config;

    public WFRuntime(Config config) {
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, WFRun> context) {
        wfRunStore = context.getStateStore(Constants.WF_RUN_STORE);
        wfSpecStore = context.getStateStore(Constants.WF_SPEC_GUID_STORE);
    }

    @Override
    public void process(final Record<String, WFEventSchema> record) {
        try {
            processHelper(record);
        } catch(Exception exn) {
            exn.printStackTrace();
        }
    }

    private void processHelper(final Record<String, WFEventSchema> record)
    throws LHConnectionError {
        String wfRunGuid = record.key();
        WFEventSchema event = record.value();

        WFRun wfRun = wfRunStore.get(wfRunGuid);
        WFSpec wfSpec = getWFSpec(event.wfSpecDigest);

        if (wfSpec == null && event.type == WFEventType.WF_RUN_STARTED) {
            throw new RuntimeException("Implement wfspec lookup");
        }

        if (wfSpec == null) {
            LHUtil.log(
                "Got an event for which we either couldn't find wfSpec:\n",
                event.toString()
            );

            // TODO: Catch the exceptions on loading wfSpec, and forward these failed
            // records to another kafka topic so we can re-process them later.
            return;
        }

        if (wfRun == null) {
            if (event.type == WFEventType.WF_RUN_STARTED) {
                wfRun = wfSpec.newRun(record);
                wfRun.setConfig(config);
                wfRun.setWFSpec(wfSpec);
            } else {
                LHUtil.logError("Couldnt find wfRun for event", event);
                return;
            }
        } else {
            wfRun.setConfig(config);
            wfRun.setWFSpec(wfSpec);
            wfRun.incorporateEvent(event);
        }

        wfRun.updateStatuses(event);

        boolean shouldAdvance = true;
        while (shouldAdvance) {
            // This call here seems redundant but it's actually not...if I don't put it here
            // then the parent thread never notices if the exception handler thread has
            // finished.
            wfRun.updateStatuses(event);
            boolean didAdvance = false;
            for (int i = 0; i < wfRun.threadRuns.size(); i++) {
                ThreadRun thread = wfRun.threadRuns.get(i);
                didAdvance = thread.advance(event) || didAdvance;
            }
            shouldAdvance = didAdvance;
            wfRun.updateStatuses(event);
        }

        wfRunStore.put(wfRun.guid, wfRun);
    }

    private WFSpec getWFSpec(String guid) throws LHConnectionError {
        return guid == null ? null : wfSpecStore.get(guid);
    }

}
