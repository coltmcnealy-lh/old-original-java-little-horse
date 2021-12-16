package little.horse.lib;

import java.util.ArrayList;
import java.util.Date;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.TaskRunEndedEventSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.TaskRunStartedEventSchema;
import little.horse.lib.schemas.WFEventSchema;
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

    private Object jsonifyIfPossible(String data) {
        try {
            Object obj = LHUtil.mapper.readValue(data, Object.class);
            return obj;
        } catch(Exception exn) {
            return data;
        }
    }

    private void raiseWorkflowProcessingError(String msg, WFEventSchema event) {
        LHUtil.logError(msg, "\n\nOrzdash just workflowprocessingerrorred\n\n");
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
            raiseWorkflowProcessingError("Failed to unmarshal WFRunRequest", event);
            return wfRun;
        }

        LHUtil.log("Got Run Request", runRequest);

        wfRun = new WFRunSchema();
        wfRun.guid = record.key();
        wfRun.wfSpecGuid = event.wfSpecGuid;
        wfRun.wfSpecName = event.wfSpecName;
        wfRun.inputVariables = runRequest.inputVariables;
        wfRun.variables = runRequest.variables;
        wfRun.status = LHStatus.RUNNING;
        wfRun.taskRuns = new ArrayList<TaskRunSchema>();

        // Figure out the input, and schedule the TaskRun.
        WFSpec wfSpec = this.getWFSpec(event.wfSpecGuid);
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
            raiseWorkflowProcessingError("Got invalid TaskRunStartedEvent", event);
        }

        // Need to find the task that just got started.
        int executionNumber = trs.taskExecutionNumber;
        LHUtil.log(Integer.valueOf(executionNumber));
        TaskRunSchema theTask = getTaskRunFromExecutionNumber(
            wfRun, executionNumber, trs.nodeGuid
        );
        if (theTask == null) {
            raiseWorkflowProcessingError(
                "Event said a task that doesn't exist yet was started.", event
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

    private WFRunSchema handleTaskCompleted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record
    ) {
        assert(event.type == WFEventType.TASK_COMPLETED);
        TaskRunEndedEventSchema tre = BaseSchema.fromString(
            event.content,
            TaskRunEndedEventSchema.class
        );
        int executionNumber = tre.taskExecutionNumber;
        String nodeGuid = tre.nodeGuid;
        TaskRunSchema task = getTaskRunFromExecutionNumber(wfRun, executionNumber, nodeGuid);

        if (task == null) {
            raiseWorkflowProcessingError(
                "Event said a task that doesn't exist yet was started.", event
            );
            return wfRun;
        }

        if (task.status != LHStatus.RUNNING) {
            raiseWorkflowProcessingError("Tried to complete a Task that wasn't running", event);
        }

        task.status = LHStatus.COMPLETED;
        task.endTime = event.timestamp;
        task.stderr = jsonifyIfPossible(tre.stderr);
        task.stdout = jsonifyIfPossible(tre.stdout);
        task.returnCode = tre.returncode;

        // Now see what we need to do from here.
        int numTaskRunsScheduled = 0;
        WFSpec wfSpec = getWFSpec(wfRun.wfSpecGuid);
        NodeSchema curNode = wfSpec.getModel().nodes.get(task.nodeName);
        for (EdgeSchema edge : curNode.outgoingEdges) {
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
            raiseWorkflowProcessingError("Unimplemented: Multiple outbound edges", event);
        }
        return wfRun;
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
            case TASK_COMPLETED:    wfRun = handleTaskCompleted(event, wfRun, record);
                                    break;
            case TASK_FAILED: break;
            case WORKFLOW_PROCESSING_FAILED: break;
            // case TASK_FAILED:       handleTaskFailed(event, wfRun, record);
            //                         break;
            // case WORKFLOW_PROCESSING_FAILED: handleWorkflowProcessingFailed(event, wfRun, record);
            //                         break;
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

    private WFSpec getWFSpec(String guid) {
        // TODO: Do some caching here
        try {
            return WFSpec.fromIdentifier(guid, config);
        } catch (Exception exn) {
            raiseError();
            return null;
        }
    }
    
    private void raiseError() {
        System.out.println("Raising error!!");
    }

    // private void raiseError(String msg) {
    //     raiseError();
    // }
    
}
