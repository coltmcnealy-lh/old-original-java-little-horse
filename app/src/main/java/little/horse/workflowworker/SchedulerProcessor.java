package little.horse.workflowworker;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.DepInjContext;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.ThreadRun;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.objects.rundata.WFRunTimer;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;


public class SchedulerProcessor
    implements Processor<String, WFEvent, String, SchedulerOutput>
{
    private KeyValueStore<String, WFRun> wfRunStore;
    private KeyValueStore<String, Bytes> timerStore;
    private WFSpec wfSpec;
    private ProcessorContext<String, SchedulerOutput> context;
    private Cancellable punctuator;
    private DepInjContext config;

    public SchedulerProcessor(DepInjContext config, WFSpec wfSpec) {
        this.wfSpec = wfSpec;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, SchedulerOutput> context) {
        wfRunStore = context.getStateStore(Constants.WF_RUN_STORE_NAME);
        timerStore = context.getStateStore(Constants.TIMER_STORE_NAME);
        this.context = context;

        punctuator = context.schedule(
            Constants.PUNCTUATOR_INERVAL,
            PunctuationType.WALL_CLOCK_TIME,
            this::punctuate
        );
    }

    @Override
    public void process(final Record<String, WFEvent> record) {
        try {
            processHelper(record.key(), record.value(), record.timestamp());
        } catch(Exception exn) {
            // TODO: Something less dumb
            exn.printStackTrace();
        }
    }

    public void punctuate(long timestamp) {
        String start = getTimerKey(0);
        String end = getTimerKey(timestamp);

        try (KeyValueIterator<String, Bytes> iter = timerStore.range(start, end)) {
            while (iter.hasNext()) {
                KeyValue<String, Bytes> entry = iter.next();
                TimerEntries entries = null;
                try {
                    entries = BaseSchema.fromBytes(
                        entry.value.get(), TimerEntries.class, config
                    );
                } catch (LHSerdeError exn) {
                    continue;
                }

                for (WFRunTimer timer: entries.timers) {
                    WFEvent event = new WFEvent();
                    event.content = timer.toString();
                    event.setConfig(config);
                    event.threadRunId = timer.threadRunId;
                    event.timestamp = new Date(timestamp);
                    event.wfRunId = timer.wfRunId;
                    event.type = WFEventType.TIMER_EVENT;

                    try {
                        processHelper(timer.wfRunId, event, timestamp);
                    } catch (LHConnectionError exn) {
                        exn.printStackTrace();
                        throw new RuntimeException("Hoboy");
                        // TODO: This is a source of potential inconsistency since
                        // we still delete the timer anyways.
                    }
                }

                timerStore.delete(entry.key);
            }
        }
    }

    @Override
    public void close() {
        punctuator.cancel();
    }

    private void processHelper(String wfRunGuid, WFEvent event, long timestamp)
    throws LHConnectionError {

        WFRun wfRun = wfRunStore.get(wfRunGuid);

        if (wfRun == null) {
            if (event.type == WFEventType.WF_RUN_STARTED) {
                wfRun = wfSpec.newRun(wfRunGuid, event);
                wfRun.setWFSpec(wfSpec);
            } else {
                // This really shouldn't happen.
                LHUtil.logError("Couldnt find wfRun for event", event);
                return;
            }
        } else {
            wfRun.setWFSpec(wfSpec);
            wfRun.incorporateEvent(event);
        }
        wfRun.updateStatuses(event);

        /*
         * The three important methods here are:
         * wfRun.incorporateEvent()
         *  -> either wfRun.handleExternalEvent() or thread.incporateEvent(event)
         *  -> May move status to STOPPING, HALTING, or RUNNING if appropriate.
         *
         * wfRun.updateStatuses(event)
         *  -> Calls updateStatus on all threadRuns
         *  -> Sets wfRun status appropriately
         *
         * thread.advance(event, toSchedule)
         *  -> schedules new tasks if necessary, checks if external events have come
         *     in, and the like.
         */

        boolean shouldAdvance = true;
        ArrayList<TaskScheduleRequest> toSchedule = new ArrayList<>();
        ArrayList<WFRunTimer> timers = new ArrayList<>();

        while (shouldAdvance) {
            // // This call here seems redundant but it's actually not...if I don't put
            // // it here then the parent thread never notices if the exception
            // // handler thread has finished.
            // // Jury still out on whether necessary.
            // wfRun.updateStatuses(event);
            boolean didAdvance = false;
            for (int i = 0; i < wfRun.threadRuns.size(); i++) {
                ThreadRun thread = wfRun.threadRuns.get(i);
                didAdvance = thread.advance(event, toSchedule, timers) || didAdvance;
            }
            shouldAdvance = didAdvance;
            wfRun.updateStatuses(event);
        }

        for (TaskScheduleRequest tsr: toSchedule) {
            SchedulerOutput co = new SchedulerOutput();
            co.request = tsr;
            context.forward(new Record<String, SchedulerOutput>(
                wfRun.getId(), co, timestamp
            ));
        }

        // Set all the timers!
        for (WFRunTimer timer: timers) {
            String k = getTimerKey(timer.maturationTimestamp);
            Bytes entryBytes = timerStore.get(k);

            TimerEntries entries;

            if (entryBytes == null) {
                entries = new TimerEntries();
                entries.timers = new ArrayList<>();
                entries.setConfig(config);
            } else {
                try {
                    entries = BaseSchema.fromBytes(
                        entryBytes.get(), TimerEntries.class, config
                    );
                } catch (LHSerdeError exn) {
                    exn.printStackTrace();
                    throw new RuntimeException("not possible");
                }
            }

            entries.timers.add(timer);

            Bytes timerBytes = new Bytes(entries.toBytes());
            timerStore.put(getTimerKey(timer.maturationTimestamp), timerBytes);
        }

        SchedulerOutput co = new SchedulerOutput();
        co.wfRun = wfRun;
        context.forward(new Record<String, SchedulerOutput>(
            wfRun.getId(), co, timestamp
        ));

        wfRunStore.put(wfRun.getId(), wfRun);
    }

    private String getTimerKey(long timestamp) {
        return String.format(Locale.US, "%020d", timestamp);
    }
}


class TimerEntries extends BaseSchema {
    public List<WFRunTimer> timers;
}
