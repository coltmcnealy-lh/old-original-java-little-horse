package little.horse.lib.wfRuntime;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHUtil;
import little.horse.lib.WFEventType;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunRequestSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFSpecSchema;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.ThreadRunSchema;


public class WFRuntime
    implements Processor<String, WFEventSchema, String, WFRunSchema>
{
    private KeyValueStore<String, WFRunSchema> wfRunStore;
    private KeyValueStore<String, WFSpecSchema> wfSpecStore;
    private Config config;

    public WFRuntime(Config config) {
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, WFRunSchema> context) {
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
    throws LHLookupException, LHNoConfigException {
        String wfRunGuid = record.key();
        WFEventSchema event = record.value();

        WFRunSchema wfRun = wfRunStore.get(wfRunGuid);
        WFSpecSchema wfSpec = getWFSpec(event.wfSpecGuid);

        if (wfSpec == null && event.type == WFEventType.WF_RUN_STARTED) {
            wfSpec = createNewWFSpec(event);
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
                ThreadRunSchema thread = wfRun.threadRuns.get(i);
                didAdvance = thread.advance(event) || didAdvance;
            }
            shouldAdvance = didAdvance;
            wfRun.updateStatuses(event);
        }

        wfRunStore.put(wfRun.guid, wfRun);
    }

    private WFSpecSchema getWFSpec(String guid) throws LHLookupException, LHNoConfigException {
        return guid == null ? null : wfSpecStore.get(guid);
    }

    private WFSpecSchema createNewWFSpec(WFEventSchema event) {
        WFRunRequestSchema req = BaseSchema.fromString(
            event.content, WFRunRequestSchema.class
        );
        if (req.wfSpec != null) {
            wfSpecStore.put(req.wfSpec.guid, req.wfSpec);
            event.wfSpecGuid = req.wfSpec.guid;
            return req.wfSpec;
        }

        return null;
    }
}
