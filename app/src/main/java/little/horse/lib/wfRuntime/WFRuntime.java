package little.horse.lib.wfRuntime;

import java.util.HashMap;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHDatabaseClient;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHUtil;
import little.horse.lib.WFEventProcessorActor;
import little.horse.lib.WFEventType;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFSpecSchema;
import little.horse.lib.schemas.ThreadRunSchema;


public class WFRuntime
    implements Processor<String, WFEventSchema, String, WFRunSchema>
{
    private KeyValueStore<String, WFRunSchema> kvStore;
    private WFEventProcessorActor actor;
    private Config config;
    private HashMap<String, WFSpecSchema> wfspecs;

    public WFRuntime(WFEventProcessorActor actor, Config config) {
        this.actor = actor;
        this.config = config;
        this.wfspecs = new HashMap<String, WFSpecSchema>();
    }

    @Override
    public void init(final ProcessorContext<String, WFRunSchema> context) {
        kvStore = context.getStateStore(Constants.WF_RUN_STORE);
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
    throws LHLookupException, LHNoConfigException {
        String wfRunGuid = record.key();
        WFEventSchema event = record.value();

        WFRunSchema wfRun = kvStore.get(wfRunGuid);
        WFSpecSchema wfSpec = getWFSpec(event.wfSpecGuid);

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

        if (shouldHalt(wfRun, event)) {
            wfRun.status = WFRunStatus.HALTING;
        } else if (shouldStart(wfRun, event)) {
            wfRun.status = WFRunStatus.RUNNING;
        }

        wfRun.updateStatuses(event);

        boolean shouldAdvance = true;
        while (shouldAdvance) {
            boolean didAdvance = false;
            for (int i = 0; i < wfRun.threadRuns.size(); i++) {
                ThreadRunSchema thread = wfRun.threadRuns.get(i);
                didAdvance = thread.advance(event, actor) || didAdvance;
            }
            shouldAdvance = didAdvance;
            wfRun.updateStatuses(event);
        }

        kvStore.put(wfRun.guid, wfRun);
    }

    private WFSpecSchema getWFSpec(String guid) throws LHLookupException, LHNoConfigException {
        if (wfspecs.get(guid) != null) {
            return wfspecs.get(guid);
        }
        // TODO: Do some caching here—that's the only reason we have this.
        WFSpecSchema result = LHDatabaseClient.lookupWFSpec(guid, config);
        result.setConfig(config);

        wfspecs.put(guid, result);
        return result;
    }

    private boolean shouldHalt(WFRunSchema wfRun, WFEventSchema event) {
        // Logic for determining whether an exception should stop the whole world or
        // just stop one thread should live here.
        return false;
    }

    private boolean shouldStart(WFRunSchema wfRun, WFEventSchema event) {
        // As of now, we don't have any "please resume" events, like manual restarts
        // or things like that.
        return false;
    }

}
