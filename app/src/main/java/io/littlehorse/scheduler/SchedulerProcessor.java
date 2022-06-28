package io.littlehorse.scheduler;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.events.WFEvent;
import io.littlehorse.common.events.WFEventType;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.model.BaseSchema;
import io.littlehorse.common.model.metadata.WFSpec;
import io.littlehorse.common.model.rundata.LHExecutionStatus;
import io.littlehorse.common.model.rundata.LHFailureReason;
import io.littlehorse.common.model.rundata.ThreadRun;
import io.littlehorse.common.model.rundata.WFRun;
import io.littlehorse.common.model.rundata.WFRunTimer;
import io.littlehorse.common.util.Constants;
import io.littlehorse.common.util.LHDatabaseClient;
import io.littlehorse.common.util.LHUtil;


public class SchedulerProcessor
    implements Processor<String, WFEvent, String, SchedulerOutput>
{
    private KeyValueStore<String, WFRun> wfRunStore;
    private KeyValueStore<String, Bytes> timerStore;
    // private WFSpec wfSpec;
    private ProcessorContext<String, SchedulerOutput> context;
    private Cancellable punctuator;
    private LHConfig config;

    private HashMap<String, WFSpec> wfSpecCache;

    public SchedulerProcessor(LHConfig config) {
        this.wfSpecCache = new HashMap<>();
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, SchedulerOutput> context) {
        wfRunStore = context.getStateStore(Constants.WF_RUN_STORE_NAME);
        timerStore = context.getStateStore(Constants.TIMER_STORE_NAME);
        this.context = context;

        punctuator = context.schedule(
            Constants.PUNCTUATOR_INERVAL,
            PunctuationType.STREAM_TIME,
            this::clearTimers
        );
    }

    @Override
    public void process(final Record<String, WFEvent> record) {
        try {
            processHelper(record.key(), record.value(), record.timestamp());
        } catch(Exception exn) {
            String wfRunId = record.key();
            WFRun wfRun = wfRunStore.get(wfRunId);
            if (wfRun == null) {
                exn.printStackTrace();
                LHUtil.logError("failed on an unknown wfrun: ", exn.getStackTrace());
                return;
            }
            try {
                wfRun.status = LHExecutionStatus.HALTED;
                wfRun.errorCode = LHFailureReason.INTERNAL_LITTLEHORSE_ERROR;
                String st = ExceptionUtils.getStackTrace(exn);
                wfRun.errorMessage = "Had an unexpected error: " + st;
                wfRunStore.put(wfRunId, wfRun);
            } catch (Exception exn2) {
                exn2.printStackTrace();
                LHUtil.logError("Had a massive orzdash");
            }
        }
    }

    public void clearTimers(long timestamp) {
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
                    event.threadId = timer.threadRunId;
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
        WFSpec wfSpec = getWFSpec(event.wfSpecId);

        if (wfSpec == null) {
            // This should REALLY not happen.
            // TODO: Create a deadletter queue.
            return;
        }

        if (wfRun == null) {
            return;
            // if (event.type == WFEventType.WF_RUN_STARTED) {
            //     wfRun = wfSpec.newRun(wfRunGuid, event);
            //     wfRun.setWFSpec(wfSpec);
            // } else {
            //     // This really shouldn't happen.
            //     LHUtil.log("Couldn't find wfRun for event", wfRunGuid);

            //     // TODO: Shoudl we maybe put some record saying it's orzdashed?
            //     return;
            // }
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
            // This call here seems redundant but it's actually not...if I don't put
            // it here then the parent thread never notices if the exception
            // handler thread has finished.
            // Jury still out on whether necessary.
            wfRun.updateStatuses(event);
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
                wfRun.getObjectId(), co, timestamp
            ), SchedulerTopology.taskQueueSink);
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
            wfRun.getObjectId(), co, timestamp
        ), SchedulerTopology.wfRunSink);

        wfRunStore.put(wfRun.getObjectId(), wfRun);
    }

    private String getTimerKey(long timestamp) {
        return String.format(Locale.US, "%020d", timestamp);
    }

    private WFSpec getWFSpec(String id) throws LHConnectionError {
        WFSpec spec = wfSpecCache.get(id);
        if (spec == null) {
            spec = LHDatabaseClient.getByNameOrId(id, config, WFSpec.class);
            wfSpecCache.put(id, spec);
        }
        return spec;
    }
}


class TimerEntries extends BaseSchema {
    public List<WFRunTimer> timers;
}
