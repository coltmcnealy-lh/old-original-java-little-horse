package little.horse.lib;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.objects.WFRun;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeSchema;
import little.horse.lib.schemas.ExternalEventPayloadSchema;
import little.horse.lib.schemas.ExternalEventThingySchema;
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

        ArrayList<TaskRunSchema> newTaskRuns = new ArrayList<TaskRunSchema>();

        switch (event.type) {
            case WF_RUN_STARTED:    handleWFRunStarted(event, wfRun, record, newTaskRuns);
                                    break;
            case TASK_STARTED:      handleTaskStarted(event, wfRun, record, newTaskRuns);
                                    break;
            case NODE_COMPLETED:    handleTaskCompleted(event, wfRun, record, newTaskRuns);
                                    break;
            case TASK_FAILED:       handleTaskFailed(event, wfRun, record, newTaskRuns);
                                    break;
            case EXTERNAL_EVENT:    handleExternalEvent(event, wfRun, record, newTaskRuns);
                                    break;
            case WORKFLOW_PROCESSING_FAILED: handleWorkflowProcessingFailed(
                                                    event, wfRun, record, newTaskRuns);
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

    /**
     * Handles an incoming External Event. There are two scenarios here:
     * 1. The WFRun has reached the Node at which this ExternalEvent is caught. In this
     *    scenario, the NodeRun is marked as COMPLETED and the payload of the ExternalEvent
     *    is saved as the stdout of the NodeRun.
     * 2. The WFRun has NOT reached the Node at which this ExternalEvent is caught. Here,
     *    we just store the fact that the Event has occurred for later use.
     * In scenario 1), we will either append new tasks to newTaskRuns or we will mark the
     * WFRun as completed.
     * @param event the WFEventSchema event to be handled. This is one event on the
     * event log.
     * @param wfRun the WFRunSchema to which `event` pertains.
     * @param record the KafkaStreams `processor.api.Record` that `event` came from.
     * @param newTaskRuns an empty list of `TaskRunSchema` that will be filled by all new TaskRun's
     * that are scheduled due to the logging of this event.
     */
    private void handleExternalEvent(
        WFEventSchema event,
        WFRunSchema wfRun,
        final Record<String, WFEventSchema> record,
        ArrayList<TaskRunSchema> newTaskRuns
    ) {
        if (wfRun.pendingEvents == null) {
            wfRun.pendingEvents = new HashMap<String, ArrayList<ExternalEventThingySchema>>();
        }

        ExternalEventThingySchema thingy = new ExternalEventThingySchema();

        ExternalEventPayloadSchema payload = BaseSchema.fromString(
            event.content,
            ExternalEventPayloadSchema.class
        );
        
        // TODO: handle if payload is null;
        if (payload == null) return;

        thingy.event = payload;

        return;
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

    /**
     * Handles a request to run a WFRun for a WFSpec. Adds the entrypoint task to `newTaskRuns`.
     * Marks the WFRun as RUNNING.
     * @param event the WFEventSchema event to be handled. This is one event on the
     * event log.
     * @param wfRun the WFRunSchema to which `event` pertains.
     * @param record the KafkaStreams `processor.api.Record` that `event` came from.
     * @param newTaskRuns an empty list of `TaskRunSchema` that will be filled by all new TaskRun's
     * that are scheduled due to the logging of this event.
     */
    private void handleWFRunStarted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<TaskRunSchema> newTaskRuns
    ) {
        assert(event.type == WFEventType.WF_RUN_STARTED);
        if (wfRun != null) {
            // It *should* be null because this is the first event in the WFRun, so
            // the lookup on the WFRun should be null.
            LHUtil.logError("Got a WFRun Started on guid", wfRun.guid, "already: ", wfRun);
            
            // TODO: Should there be some notification / should we orzdash the actual wfRun
            // that's currently running?
            return;
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
            return;
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
            return;
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
        return;
    }

    /**
     * Handles an event noting that a Node has begun its processing. Marks the NodeRun as RUNNING.
     * Should not add anything to newTaskRuns unless some weird shit happens.
     * @param event the WFEventSchema event to be handled. This is one event on the
     * event log.
     * @param wfRun the WFRunSchema to which `event` pertains.
     * @param record the KafkaStreams `processor.api.Record` that `event` came from.
     * @param newTaskRuns an empty list of `TaskRunSchema` that will be filled by all new TaskRun's
     * that are scheduled due to the logging of this event.
     */
    private void handleTaskStarted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<TaskRunSchema> newTaskRuns
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
            return;
        }

        // Ok, now we have the task.
        theTask.status = LHStatus.RUNNING;
        theTask.startTime = event.timestamp;
        theTask.bashCommand = trs.bashCommand;
        theTask.stdin = trs.stdin;

        return;
    }

    /**
     * Handles an event marking a NodeRun as completed. If there are outgoing edges, the active
     * ones are added to the newTaskRuns list. If there are none, then the wfRun is marked as
     * COMPLETED.
     * @param event the WFEventSchema event to be handled. This is one event on the
     * event log.
     * @param wfRun the WFRunSchema to which `event` pertains.
     * @param record the KafkaStreams `processor.api.Record` that `event` came from.
     * @param newTaskRuns an empty list of `TaskRunSchema` that will be filled by all new TaskRun's
     * that are scheduled due to the logging of this event.
     */
    private void handleTaskCompleted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<TaskRunSchema> newTaskRuns
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
            return;
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
            return;
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
        return;
    }

    /**
     * Handles an event marking a task as failed. Depending on the error handling for the workflow
     * (in the future we may have rollbacks, retries, retries w/backoff, etc.) there may or may not
     * be tasks appended to newTaskRuns. If there are no error-handling things to do afterwards (this
     * is the current state of the system) then the wfRun is marked as ERROR.
     * @param event the WFEventSchema event to be handled. This is one event on the
     * event log.
     * @param wfRun the WFRunSchema to which `event` pertains.
     * @param record the KafkaStreams `processor.api.Record` that `event` came from.
     * @param newTaskRuns an empty list of `TaskRunSchema` that will be filled by all new TaskRun's
     * that are scheduled due to the logging of this event.
     */
    private void handleTaskFailed(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<TaskRunSchema> newTaskRuns
    ) {
        TaskRunFailedEventSchema trf = BaseSchema.fromString(
            event.content, TaskRunFailedEventSchema.class
        );

        if (trf == null) {
            raiseWorkflowProcessingError(
                "Got invalid failure event " + event.toString(), event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return;
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
            return;
        }

        tr.returnCode = trf.returncode;
        tr.endTime = event.timestamp;
        tr.failureMessage = trf.message;
        tr.failureReason = trf.reason;
        tr.stdout = jsonifyIfPossible(trf.stdout);
        tr.stderr = jsonifyIfPossible(trf.stderr);
        tr.status = LHStatus.ERROR;

        return;
    }

    /**
     * Handles an event saying that there was an internal error (likely caused by a bug or an
     * invalid WFSpec) making it impossible to determine what to do for the wfRun. Currently
     * just marks the wfRun as ERROR and terminates it. In the future, it may handle rollbacks
     * of SAGA transactions, etc.
     * @param event the WFEventSchema event to be handled. This is one event on the
     * event log.
     * @param wfRun the WFRunSchema to which `event` pertains.
     * @param record the KafkaStreams `processor.api.Record` that `event` came from.
     * @param newTaskRuns an empty list of `TaskRunSchema` that will be filled by all new TaskRun's
     * that are scheduled due to the logging of this event.
     */
    private void handleWorkflowProcessingFailed(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<TaskRunSchema> newTaskRuns
    ) {
        assert (event.type == WFEventType.WORKFLOW_PROCESSING_FAILED);

        WFProcessingErrorSchema errSchema = BaseSchema.fromString(
            event.content, WFProcessingErrorSchema.class
        );

        wfRun.status = LHStatus.ERROR;
        wfRun.errorCode = errSchema.reason;
        wfRun.errorMessage = errSchema.message;
        return;
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
