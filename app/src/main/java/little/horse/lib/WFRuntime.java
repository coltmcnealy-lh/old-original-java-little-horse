package little.horse.lib;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
import little.horse.lib.schemas.ExternalEventCorrelSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.SignalHandlerSpecSchema;
import little.horse.lib.schemas.NodeCompletedEventSchema;
import little.horse.lib.schemas.TaskRunFailedEventSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.TaskRunStartedEventSchema;
import little.horse.lib.schemas.VariableMutationSchema;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFProcessingErrorSchema;
import little.horse.lib.schemas.WFRunRequestSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFRunVariableDefSchema;
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
        WFSpec wfSpec = null;
        if (wfRun != null) {
            wfSpec = getWFSpec(wfRun.wfSpecGuid);
        }

        if (wfSpec == null && event.type != WFEventType.WF_RUN_STARTED) {
            raiseWorkflowProcessingError(
                "Unable to find WFSpec",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return;
        }
        
        ArrayList<NodeSchema> activatedNodes = new ArrayList<NodeSchema>();

        switch (event.type) {
            case WF_RUN_STARTED:    wfRun = handleWFRunStarted(
                                        event, wfRun, record, activatedNodes, wfSpec);
                                    break;
            case TASK_STARTED:      wfRun = handleTaskStarted(
                                        event, wfRun, record, activatedNodes, wfSpec);
                                    break;
            case NODE_COMPLETED:    wfRun = handleTaskCompleted(
                                        event, wfRun, record, activatedNodes, wfSpec);
                                    break;
            case TASK_FAILED:       wfRun = handleTaskFailed(
                                        event, wfRun, record, activatedNodes, wfSpec);
                                    break;
            case EXTERNAL_EVENT:    wfRun = handleExternalEvent(
                                        event, wfRun, record, activatedNodes, wfSpec);
                                    break;
            case WORKFLOW_PROCESSING_FAILED: wfRun = handleWorkflowProcessingFailed(
                                                    event, wfRun, record, activatedNodes, wfSpec);
                                    break;
        }

        while (activatedNodes.size() > 0) {
            ArrayList<NodeSchema> newActivatedNodes = new ArrayList<NodeSchema>();

            for (NodeSchema node : activatedNodes) {
                kvStore.put(wfRun.guid, wfRun); // TODO: Might not be necessary here.
                processNode(node, wfRun, wfSpec, activatedNodes, event, newActivatedNodes);
            }

            activatedNodes = newActivatedNodes;
        }

        if (wfRun.status == LHStatus.RUNNING) {
            boolean allDone = true;
            for (TaskRunSchema tr : wfRun.taskRuns) {
                if (tr.status != LHStatus.COMPLETED) {
                    allDone = false;
                }
            }
            if (allDone) {
                wfRun.status = LHStatus.COMPLETED;
            }
        }

        kvStore.put(wfRun.guid, wfRun);

        Date timestamp = (event.timestamp == null)
            ? new Date(record.timestamp())
            : event.timestamp;
        context.forward(new Record<String, WFRunSchema>(
            wfRunGuid, wfRun, timestamp.getTime()
        ));
    }

    private void processNode(
        NodeSchema node,
        WFRunSchema wfRun,
        WFSpec wfSpec,
        ArrayList<NodeSchema> oldActivatedNodes,
        WFEventSchema event,
        ArrayList<NodeSchema> activatedNodes
    ) {
        if (node.nodeType == NodeType.TASK) {
            // Add the taskrun thing and execute the darn task and be done with it
            TaskRunSchema tr = new TaskRunSchema();
            tr.nodeGuid = node.guid;
            tr.nodeName = node.name;
            tr.wfRunGuid = wfRun.guid;
            tr.wfSpecGuid = wfRun.wfSpecGuid;
            tr.wfSpecName = wfRun.wfSpecName;
            tr.status = LHStatus.PENDING;
            tr.number = wfRun.taskRuns.size();

            // LHUtil.log("about to add this taskrun: ", tr);
            wfRun.taskRuns.add(tr);
            if (node.guid.equals(actor.getNodeGuid())) { // THIS IS WHERE THE WORK GETS DONE

                actor.act(wfRun, event, tr.number);
            }
            return;
        }

        if (node.nodeType == NodeType.EXTERNAL_EVENT) {
            // Two situations are possible here: the WFRun got here before the event, or
            // the event already got here.

            if (wfRun.pendingEvents == null) {
                wfRun.pendingEvents = new HashMap<String, ArrayList<ExternalEventCorrelSchema>>();
            }

            TaskRunSchema tr = new TaskRunSchema();
            tr.nodeGuid = node.guid;
            tr.nodeName = node.name;
            tr.wfRunGuid = wfRun.guid;
            tr.wfSpecGuid = wfRun.wfSpecGuid;
            tr.wfSpecName = wfRun.wfSpecName;
            tr.number = wfRun.taskRuns.size();

            wfRun.taskRuns.add(tr);

            // Check wfRun.pendingEvents to see if there's one waiting.
            ArrayList<ExternalEventCorrelSchema> relevantEvents = wfRun.pendingEvents.get(
                node.externalEventDefName
            );
            if (relevantEvents == null) {
                relevantEvents = new ArrayList<ExternalEventCorrelSchema>();
                wfRun.pendingEvents.put(node.externalEventDefName, relevantEvents);
            }

            ExternalEventCorrelSchema correlSchema = null;
            for (ExternalEventCorrelSchema candidate : relevantEvents) {
                if (candidate.event != null && candidate.assignedNodeGuid == null) {
                    correlSchema = candidate;
                }
            }

            if (correlSchema == null) {
                correlSchema = new ExternalEventCorrelSchema();
                relevantEvents.add(correlSchema);

                tr.startTime = event.timestamp;
                tr.status = LHStatus.WAITING_FOR_EVENT;
            } else {
                // TODO: We need to close out the taskRun and fire off outbound edges.

                tr.endTime = event.timestamp;
                tr.status = LHStatus.COMPLETED;
                
                try {
                    mutateVariables(wfRun, node.variableMutations, tr, wfSpec);
                    for (EdgeSchema edge : node.outgoingEdges) {
                        if (WFRun.evaluateEdge(wfRun, edge.condition)) {
                            activatedNodes.add(wfSpec.getModel().nodes.get(edge.sinkNodeName));
                        }
                    }
                } catch(VarSubOrzDash exn) {
                    exn.printStackTrace();
                    raiseWorkflowProcessingError(
                        "Failed substituting variable on outgoing edge from node " + node.name,
                        event,
                        LHFailureReason.VARIABLE_LOOKUP_ERROR
                    );
                }

            }

            correlSchema.assignedNodeGuid = node.guid;
            correlSchema.assignedNodeName = node.name;
            correlSchema.assignedTaskRunExecutionNumber = tr.number;
        }
    }

    private WFRunSchema handleExternalEvent(
        WFEventSchema event,
        WFRunSchema wfRun,
        final Record<String, WFEventSchema> record,
        ArrayList<NodeSchema> activatedNodes, WFSpec wfSpec
    ) {
        ExternalEventPayloadSchema payload = BaseSchema.fromString(
            event.content,
            ExternalEventPayloadSchema.class
        );

        if (payload == null) {
            raiseWorkflowProcessingError(
                "invalid event", event, LHFailureReason.INVALID_WF_SPEC_ERROR
            );
            return wfRun;
        }

        // determine whether to handle this event as an interrupt or to delegate it to a node.

        SignalHandlerSpecSchema relevantHandler = null;
        for (SignalHandlerSpecSchema handler: wfSpec.getModel().signalHandlers) {
            if (handler.externalEventDefGuid.equals(payload.externalEventDefGuid)) {
                relevantHandler = handler;
                break;
            }
        }

        if (relevantHandler != null) {
            // Then we check to see if we have any PENDING or RUNNING taskRuns.

            // If no PENDING or RUNNING taskRuns, we then mark the wfRun as INTERRUPTED and
            // kick off the signalhandler workflow. We also add it to the interrupt stack.

            // if there is a PENDING or RUNNING taskRun, we mark the wfRun as PENDING_INTERRUPT
            // and put the signalHandler on the stack of interrupts.
        }

        // If we got this far, then we know we're supposed to match the externalEvent to
        // a specific node (i.e. there's no signalhandler for it.)
        if (wfRun.pendingEvents == null) {
            wfRun.pendingEvents = new HashMap<String, ArrayList<ExternalEventCorrelSchema>>();
        }

        ArrayList<ExternalEventCorrelSchema> relevantCorrels = wfRun.pendingEvents.get(
            payload.externalEventDefName);

        if (relevantCorrels == null) {
            wfRun.pendingEvents.put(
                payload.externalEventDefName, new ArrayList<ExternalEventCorrelSchema>()
            );
        }

        for (ExternalEventCorrelSchema correlSchema :
            wfRun.pendingEvents.get(payload.externalEventDefName)
        ) {
            // Now we look to see if the wfRun has already got here and is waiting.
            if (correlSchema.assignedNodeGuid != null && correlSchema.event == null) {
                correlSchema.event = payload;

                // Now we need to complete the NodeRun.
                TaskRunSchema tr = getTaskRunFromGuid(
                    wfRun, correlSchema.assignedTaskRunExecutionNumber
                );
                tr.stdout = payload.content;
                tr.endTime = new Date(record.timestamp());
                tr.status = LHStatus.COMPLETED;

                
                NodeSchema node = wfSpec.getModel().nodes.get(correlSchema.assignedNodeName);
                // TODO: handle if node is null;
                
                
                // Now we fire outgoing edges if necessary.
                try {
                    mutateVariables(wfRun, node.variableMutations, tr, wfSpec);
                    for (EdgeSchema edge : node.outgoingEdges) {
                        if (WFRun.evaluateEdge(wfRun, edge.condition)) {
                            activatedNodes.add(wfSpec.getModel().nodes.get(edge.sinkNodeName));
                        }
                    }
                } catch(VarSubOrzDash exn) {
                    exn.printStackTrace();
                    raiseWorkflowProcessingError(
                        "Failed substituting variable on outgoing edge from node " + node.name,
                        event,
                        LHFailureReason.VARIABLE_LOOKUP_ERROR
                    );
                }
                return wfRun;
            }
        }

        // if we get this far, then we know that the event got here before the wfRun.
        // That means we just have to store the event for later processing in processNode.
        ExternalEventCorrelSchema schema = new ExternalEventCorrelSchema();
        schema.event = payload;
        wfRun.pendingEvents.get(payload.externalEventDefName).add(schema);

        return wfRun;
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
            final Record<String, WFEventSchema> record,
            ArrayList<NodeSchema> activatedNodes, WFSpec wfSpec
    ) {
        assert(event.type == WFEventType.WF_RUN_STARTED);
        if (wfRun != null) {
            // It *should* be null because this is the first event in the WFRun, so
            // the lookup on the WFRun should be null.
            LHUtil.logError("Got a WFRun Started on guid", wfRun.guid, "already: ", wfRun);

            // wfSpec should be null at this point due to weirdness.
            if (wfSpec == null) {
                raiseWorkflowProcessingError(
                    "Unable to find WFSpec",
                    event,
                    LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
                );
                return wfRun;
            }

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

        // Now we finally know enough to get the wfSpec.
        wfSpec = getWFSpec(event.wfSpecGuid);
        if (wfSpec == null) {
            raiseWorkflowProcessingError(
                "Unable to find WFSpec",
                event,
                LHFailureReason.INTERNAL_LITTLEHORSE_ERROR
            );
            return wfRun;
        }

        wfRun = new WFRunSchema();
        wfRun.guid = record.key();
        wfRun.wfSpecGuid = event.wfSpecGuid;
        wfRun.wfSpecName = event.wfSpecName;
        wfRun.status = LHStatus.RUNNING;
        wfRun.taskRuns = new ArrayList<TaskRunSchema>();
        
        wfRun.variables = runRequest.variables;
        wfRun.variables = new HashMap<String, Object>();
        if (runRequest.variables == null) {
            runRequest.variables = new HashMap<String, Object>();
        }
        for (String varName: wfSpec.getModel().variableDefs.keySet()) {
            WFRunVariableDefSchema varDef = wfSpec.getModel().variableDefs.get(varName);

            Object result = runRequest.variables.get(varName);
            if (result != null) {
                wfRun.variables.put(varName, result);
            } else {
                wfRun.variables.put(varName, varDef.defaultValue);
            }
        }

        WFSpecSchema wfSpecSchema = wfSpec.getModel();
        NodeSchema node = wfSpecSchema.nodes.get(
            wfSpecSchema.entrypointNodeName
        );

        activatedNodes.add(node);

        return wfRun;
    }

    private WFRunSchema handleTaskStarted(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<NodeSchema> outgoingEdges, WFSpec wfSpec
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
        // int executionNumber = trs.taskExecutionNumber;

        TaskRunSchema theTask = getTaskRunFromGuid(
            wfRun, trs.taskRunNumber
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
            final Record<String, WFEventSchema> record,
            ArrayList<NodeSchema> activatedNodes, WFSpec wfSpec
    ) {
        assert(event.type == WFEventType.NODE_COMPLETED);
        NodeCompletedEventSchema tre = BaseSchema.fromString(
            event.content,
            NodeCompletedEventSchema.class
        );
        TaskRunSchema task = getTaskRunFromGuid(wfRun, tre.taskRunNumber);

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
        NodeSchema curNode = wfSpec.getModel().nodes.get(task.nodeName);

        
        try {
            mutateVariables(wfRun, curNode.variableMutations, task, wfSpec);
            for (EdgeSchema edge : curNode.outgoingEdges) {
                if (WFRun.evaluateEdge(wfRun, edge.condition)) {
                    activatedNodes.add(wfSpec.getModel().nodes.get(edge.sinkNodeName));
                }
            }
        } catch (VarSubOrzDash exn) {
            raiseWorkflowProcessingError(
                exn.message, event, LHFailureReason.VARIABLE_LOOKUP_ERROR
                );
                return wfRun;
            }

        return wfRun;
    }

    private WFRunSchema handleTaskFailed(
            WFEventSchema event,
            WFRunSchema wfRun,
            final Record<String, WFEventSchema> record,
            ArrayList<NodeSchema> activatedNodes, WFSpec wfSpec
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

        TaskRunSchema tr = getTaskRunFromGuid(
            wfRun, trf.taskRunNumber
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
            final Record<String, WFEventSchema> record,
            ArrayList<NodeSchema> activatedNodes, WFSpec wfSpec
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

    private void mutateVariables(
        WFRunSchema wfRun,
        Map<String, VariableMutationSchema> mutations,
        TaskRunSchema tr,
        WFSpec wfSpec
    ) throws VarSubOrzDash {
        if (mutations == null) return;
        for (String varName : mutations.keySet()) {
            VariableMutationSchema mutation = mutations.get(varName);
            WFRun.mutateVariable(wfRun, varName, mutation, wfSpec, tr);
        }
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

    private TaskRunSchema getTaskRunFromGuid(
        WFRunSchema wfRun,
        int number
    ) {
        return wfRun.taskRuns.get(number);
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
