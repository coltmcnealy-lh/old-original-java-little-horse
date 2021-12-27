package little.horse.lib;

import java.util.ArrayList;
import java.util.Date;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.objects.WFRun;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.NodeCompletedEventSchema;
import little.horse.lib.schemas.TaskRunFailedEventSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.TaskRunStartedEventSchema;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFProcessingErrorSchema;
import little.horse.lib.schemas.WFRunRequestSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFSpecSchema;

public class WFRuntime
    implements Processor<String, WFEventSchema, String, WFRunSchema>
{
    private KeyValueStore<String, WFRunSchema> kvStore;
    private ProcessorContext<String, WFRunSchema> context;
    private WFEventProcessorActor actor;
    private Config config;

    public WFRuntime(WFEventProcessorActor actor, Config config) {
        this.actor = actor;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, WFRunSchema> context) {
        kvStore = context.getStateStore(Constants.WF_RUN_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, WFEventSchema> record) {
        // First, we gotta update the WFRun.
        String wfRunGuid = record.key();
        WFEventSchema event = record.value();

        WFRunSchema wfRun = kvStore.get(wfRunGuid);

        switch (event.type) {
            case WF_RUN_STARTED:    wfRun = handleWFRunStarted(event, wfRun, record);
                                    break;
            case TASK_STARTED:      wfRun = handleTaskStarted(event, wfRun, record);
                                    break;
            case NODE_COMPLETED:    wfRun = handleTaskCompleted(event, wfRun, record);
                                    break;
            case TASK_FAILED:       wfRun = handleTaskFailed(event, wfRun, record);
                                    break;
            case EXTERNAL_EVENT:    System.out.println("TODO");
                                    break;
            case WORKFLOW_PROCESSING_FAILED: wfRun = handleWorkflowProcessingFailed(event, wfRun, record);
                                    break;
        }
        kvStore.put(wfRun.guid, wfRun);

        actor.act(wfRun, event);

        Date timestamp = (event.timestamp == null)
            ? new Date(record.timestamp())
            : event.timestamp;
        context.forward(new Record<String, WFRunSchema>(
            wfRunGuid, wfRun, timestamp.getTime()
        ));
    }

    private void raiseWorkflowProcessingError(
            String msg, WFEventSchema event, LHFailureReason reason
    ) {
        WFEventSchema newEvent = new WFEventSchema();
        newEvent.timestamp = LHUtil.now();
        newEvent.wfRunGuid = event.wfRunGuid;
        newEvent.wfSpecGuid = event.wfSpecGuid;
        newEvent.wfSpecName = event.wfSpecName;
        newEvent.type = WFEventType.WORKFLOW_PROCESSING_FAILED;

        WFProcessingErrorSchema err = new WFProcessingErrorSchema();
        err.message = msg;
        err.reason = reason;

        newEvent.content = err.toString();
        WFSpec wfSpec = getWFSpec(event.wfRunGuid);
        if (wfSpec == null) {
            // TODO: we can't figure out the kafka topic now but we should still
            // report the polluted record.
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(
            wfSpec.getModel().kafkaTopic,
            event.wfRunGuid,
            newEvent.toString()
        );

        config.send(record);
    }

    private WFRunSchema handleWFRunStarted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record
    ) {
        assert(event.type == WFEventType.WF_RUN_STARTED);
        if (wfRun != null) {
            // It *should* be null because this is the first event in the WFRun, so
            // the lookup on the WFRun should be null.
            LHUtil.logError("Got a WFRun Started on guid", wfRun.guid, "already: ", wfRun);
            
            // TODO: Should there be some notification / should we orzdash the actual wfRun
            // that's currently running?
            return wfRun;
        }

        WFRunRequestSchema runRequest = BaseSchema.fromString(
            event.content, WFRunRequestSchema.class
        );

        if (runRequest == null) {
            raiseWorkflowProcessingError(
                "Failed to unmarshal WFRunRequest",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }

        LHUtil.log("Got Run Request", runRequest);

        wfRun = new WFRunSchema();
        wfRun.guid = record.key();
        wfRun.wfSpecGuid = event.wfSpecGuid;
        wfRun.wfSpecName = event.wfSpecName;
        wfRun.variables = runRequest.variables;
        wfRun.status = LHStatus.RUNNING;
        wfRun.taskRuns = new ArrayList<TaskRunSchema>();

        // Figure out the input, and schedule the TaskRun.
        WFSpec wfSpec = this.getWFSpec(event.wfSpecGuid);
        if (wfSpec == null) {
            raiseWorkflowProcessingError(
                "Unable to find associated WFSpec",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }
        WFSpecSchema wfSpecSchema = wfSpec.getModel();
        NodeSchema node = wfSpecSchema.nodes.get(
            wfSpecSchema.entrypointNodeName
        );

        TaskRunSchema tr = new TaskRunSchema();
        tr.wfSpecGuid = event.wfSpecGuid;
        tr.wfRunGuid = event.wfRunGuid;
        tr.wfSpecName = event.wfSpecName;
        tr.nodeName = node.name;
        tr.nodeGuid = node.guid;
        tr.executionNumber = 0;
        tr.status = LHStatus.PENDING;
        wfRun.taskRuns.add(tr);

        LHUtil.log("Just added wfRun: ", wfRun);
        return wfRun;
    }

    private WFRunSchema handleTaskStarted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record
    ) {
        assert(event.type == WFEventType.TASK_STARTED);
        TaskRunStartedEventSchema trs = BaseSchema.fromString(
            event.content,
            TaskRunStartedEventSchema.class
        );
        if (trs == null) {
            raiseWorkflowProcessingError(
                "Got invalid TaskRunStartedEvent",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
        }

        // Need to find the task that just got started.
        int executionNumber = trs.taskExecutionNumber;

        TaskRunSchema theTask = getTaskRunFromExecutionNumber(
            wfRun, executionNumber, trs.nodeGuid
        );
        if (theTask == null) {
            raiseWorkflowProcessingError(
                "Event said a task that doesn't exist yet was started.",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }

        // Ok, now we have the task.
        theTask.status = LHStatus.RUNNING;
        theTask.startTime = event.timestamp;
        theTask.bashCommand = trs.bashCommand;
        theTask.stdin = trs.stdin;

        return wfRun;
    }

    private WFRunSchema handleTaskCompleted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record
    ) {
        assert(event.type == WFEventType.NODE_COMPLETED);
        NodeCompletedEventSchema tre = BaseSchema.fromString(
            event.content,
            NodeCompletedEventSchema.class
        );
        int executionNumber = tre.taskExecutionNumber;
        String nodeGuid = tre.nodeGuid;
        TaskRunSchema task = getTaskRunFromExecutionNumber(wfRun, executionNumber, nodeGuid);

        if (task == null) {
            raiseWorkflowProcessingError(
                "Event said a task that doesn't exist yet was started.", event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }

        if (task.status != LHStatus.RUNNING) {
            raiseWorkflowProcessingError(
                "Tried to complete a Task that wasn't running",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
        }

        task.status = LHStatus.COMPLETED;
        task.endTime = event.timestamp;
        task.stderr = jsonifyIfPossible(tre.stderr);
        task.stdout = jsonifyIfPossible(tre.stdout);
        task.returnCode = tre.returncode;

        // Now see what we need to do from here.
        int numTaskRunsScheduled = 0;
        WFSpec wfSpec = getWFSpec(wfRun.wfSpecGuid);
        if (wfSpec == null) {
            raiseWorkflowProcessingError(
                "Unable to find WFSpec",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }
        NodeSchema curNode = wfSpec.getModel().nodes.get(task.nodeName);
        for (EdgeSchema edge : curNode.outgoingEdges) {
            try {
                if (!WFRun.evaluateEdge(wfRun, edge.condition)) continue;
            } catch(VarSubOrzDash exn) {
                raiseWorkflowProcessingError(
                    exn.message, event, LHFailureReason.VARIABLE_LOOKUP_ERROR
                );
            }
            // TODO: for conditional branching, decide whether edge is active.
            numTaskRunsScheduled++;
            TaskRunSchema tr = new TaskRunSchema();
            NodeSchema destNode = wfSpec.getModel().nodes.get(edge.sinkNodeName);
            tr.nodeGuid = destNode.guid;
            tr.nodeName = destNode.name;
            tr.status = LHStatus.PENDING;
            tr.wfSpecGuid = event.wfSpecGuid;
            tr.wfRunGuid = event.wfRunGuid;
            tr.wfSpecName = event.wfSpecName;
            tr.executionNumber = task.executionNumber + 1; // TODO: Won't work with threading.

            wfRun.taskRuns.add(tr);
        }
        if (numTaskRunsScheduled == 0) {
            wfRun.status = LHStatus.COMPLETED;
        } else if (numTaskRunsScheduled == 1) {
            // Nothing to do.
        } else {
            raiseWorkflowProcessingError(
                "Unimplemented: Multiple outbound edges",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
        }
        return wfRun;
    }

    private WFRunSchema handleTaskFailed(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record
    ) {
        TaskRunFailedEventSchema trf = BaseSchema.fromString(
            event.content, TaskRunFailedEventSchema.class
        );

        if (trf == null) {
            raiseWorkflowProcessingError(
                "Got invalid failure event " + event.toString(), event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }

        wfRun.status = LHStatus.ERROR;

        TaskRunSchema tr = getTaskRunFromExecutionNumber(
            wfRun, trf.taskExecutionNumber, trf.nodeGuid
        );
        if (tr == null) {
            raiseWorkflowProcessingError(
                "Got a reference to a failed task that doesn't exist " + event.toString(),
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }

        tr.returnCode = trf.returncode;
        tr.endTime = event.timestamp;
        tr.failureMessage = trf.message;
        tr.failureReason = trf.reason;
        tr.stdout = jsonifyIfPossible(trf.stdout);
        tr.stderr = jsonifyIfPossible(trf.stderr);
        tr.status = LHStatus.ERROR;

        return wfRun;
    }

    private WFRunSchema handleWorkflowProcessingFailed(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record
    ) {
        assert (event.type == WFEventType.WORKFLOW_PROCESSING_FAILED);

        WFProcessingErrorSchema errSchema = BaseSchema.fromString(
            event.content, WFProcessingErrorSchema.class
        );

        wfRun.status = LHStatus.ERROR;
        wfRun.errorCode = errSchema.reason;
        wfRun.errorMessage = errSchema.message;
        return wfRun;
    }

    // Below are a bunch of utility methods.
    private WFSpec getWFSpec(String guid) {
        // TODO: Do some caching here—that's the only reason we have this.
        try {
            return WFSpec.fromIdentifier(guid, config);
        } catch (Exception exn) {
            return null;
        }
    }

    private TaskRunSchema getTaskRunFromExecutionNumber(
        WFRunSchema wfRun,
        int executionNumber,
        String nodeGuid
    ) {
        TaskRunSchema theTask = null;
        if (wfRun.taskRuns.size() > executionNumber) {
            TaskRunSchema candidate = wfRun.taskRuns.get(executionNumber);
            if (candidate.nodeGuid.equals(nodeGuid)) {
                theTask = candidate;
            }
        }
        return theTask;
    }

    private Object jsonifyIfPossible(String data) {
        try {
            Object obj = LHUtil.mapper.readValue(data, Object.class);
            return obj;
        } catch(Exception exn) {
            return data;
        }
    }
}
