package little.horse.api.runtime;

import java.util.ArrayList;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.Config;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.ThreadRun;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;


public class WFRuntime
    implements Processor<String, WFEvent, String, CoordinatorOutput>
{
    private KeyValueStore<String, WFRun> wfRunStore;
    private WFSpec wfSpec;
    private ProcessorContext<String, CoordinatorOutput> context;

    public WFRuntime(Config config, WFSpec wfSpec) {
        this.wfSpec = wfSpec;
    }

    @Override
    public void init(final ProcessorContext<String, CoordinatorOutput> context) {
        wfRunStore = context.getStateStore(Constants.WF_RUN_STORE_NAME);
        this.context = context;
    }

    @Override
    public void process(final Record<String, WFEvent> record) {
        try {
            processHelper(record);
        } catch(Exception exn) {
            exn.printStackTrace();
        }
    }

    private void processHelper(final Record<String, WFEvent> record)
    throws LHConnectionError {
        String wfRunGuid = record.key();
        WFEvent event = record.value();

        WFRun wfRun = wfRunStore.get(wfRunGuid);

        if (wfRun == null) {
            if (event.type == WFEventType.WF_RUN_STARTED) {
                wfRun = wfSpec.newRun(record.key(), record.value());
                wfRun.setWFSpec(wfSpec);
            } else {
                LHUtil.logError("Couldnt find wfRun for event", event);
                return;
            }
        } else {
            wfRun.setWFSpec(wfSpec);
            wfRun.incorporateEvent(event);
        }

        wfRun.updateStatuses(event);

        boolean shouldAdvance = true;
        ArrayList<TaskScheduleRequest> toSchedule = new ArrayList<>();

        while (shouldAdvance) {
            // This call here seems redundant but it's actually not...if I don't put it here
            // then the parent thread never notices if the exception handler thread has
            // finished.
            wfRun.updateStatuses(event);
            boolean didAdvance = false;
            for (int i = 0; i < wfRun.threadRuns.size(); i++) {
                ThreadRun thread = wfRun.threadRuns.get(i);
                didAdvance = thread.advance(event, toSchedule) || didAdvance;
            }
            shouldAdvance = didAdvance;
            wfRun.updateStatuses(event);
        }

        for (TaskScheduleRequest tsr: toSchedule) {
            LHUtil.log("\n\n\n", tsr.taskDefName);
            CoordinatorOutput co = new CoordinatorOutput();
            co.request = tsr;
            context.forward(new Record<String, CoordinatorOutput>(
                wfRun.getId(), co, record.timestamp()
            ));
        }

        CoordinatorOutput co = new CoordinatorOutput();
        co.wfRun = wfRun;
        context.forward(new Record<String, CoordinatorOutput>(
            wfRun.getId(), co, record.timestamp()
        ));

        wfRunStore.put(wfRun.getId(), wfRun);
    }
}
