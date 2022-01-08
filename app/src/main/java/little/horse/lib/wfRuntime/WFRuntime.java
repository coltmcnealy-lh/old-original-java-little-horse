package little.horse.lib.wfRuntime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHFailureReason;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.NodeType;
import little.horse.lib.VarSubOrzDash;
import little.horse.lib.WFEventProcessorActor;
import little.horse.lib.WFEventType;
import little.horse.lib.objects.WFRun;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeSchema;
import little.horse.lib.schemas.ExternalEventCorrelSchema;
import little.horse.lib.schemas.ExternalEventPayloadSchema;
import little.horse.lib.schemas.NodeCompletedEventSchema;
import little.horse.lib.schemas.NodeSchema;
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
import little.horse.lib.schemas.WFTokenSchema;


public class WFRuntime
    implements Processor<String, WFEventSchema, String, WFRunSchema>
{
    private KeyValueStore<String, WFRunSchema> kvStore;
    private WFEventProcessorActor actor;
    private Config config;

    public WFRuntime(WFEventProcessorActor actor, Config config) {
        this.actor = actor;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, WFRunSchema> context) {
        kvStore = context.getStateStore(Constants.WF_RUN_STORE);
    }

    @Override
    public void process(final Record<String, WFEventSchema> record) {
        String wfRunGuid = record.key();
        WFEventSchema event = record.value();

        WFRunSchema wfRun = kvStore.get(wfRunGuid);
        WFSpec wfSpec = null;
        if (wfRun != null) {
            wfSpec = getWFSpec(wfRun.wfSpecGuid);
        }

        if (wfSpec == null && event.type != WFEventType.WF_RUN_STARTED) {
            LHUtil.log(
                "Got an event for which we either couldn't find wfRun or couldnt find wfSpec:\n",
                event.toString()
            );
            return;
        }

        if (wfRun == null) {
            wfRun = handleWFRunStarted(event, wfRun, record, wfSpec);
            wfSpec = getWFSpec(wfRun.wfSpecGuid);
        } else {
            updateTaskRunsOrEvents(event, wfRun, record, wfSpec);
        }

        if (shouldHalt(wfRun, event)) {
            wfRun.status = WFRunStatus.HALTING;
        } else if (shouldStart(wfRun, event)) {
            wfRun.status = WFRunStatus.RUNNING;
        }

        updateStatuses(wfRun);
        HashSet<String> alreadySeen = new HashSet<String>();
        WFTokenSchema tokenToAdvance = getTokenToAdvance(wfRun, alreadySeen);
        while (tokenToAdvance != null) {
            advanceToken(wfRun, tokenToAdvance, wfSpec, event);
            updateStatuses(wfRun);
            tokenToAdvance = getTokenToAdvance(wfRun, alreadySeen);
        }

        kvStore.put(wfRun.guid, wfRun);
    }

    public static WFTokenSchema getTokenToAdvance(WFRunSchema wfRun, Set<String> alreadySeen) {
        for (WFTokenSchema token: wfRun.tokens) {
            String key = String.valueOf(token.tokenNumber) + "__" + String.valueOf(
                token.taskRuns.size()
            );
            if (!alreadySeen.contains(key)) {
                alreadySeen.add(key);
                return token;
            }
        }

        return null;
    }

    private void updateStatuses(WFRunSchema wfRun) {

        for (WFTokenSchema token: wfRun.tokens) {
            if (token.taskRuns.size() == 0) {
                LHUtil.log("WTF?");
                continue;
            }
            if (token.upNext == null) {
                token.upNext = new ArrayList<TaskRunSchema>();
            }
            TaskRunSchema lastTr = token.taskRuns.get(token.taskRuns.size() - 1);
            if (token.status == WFRunStatus.COMPLETED) {
                // As of now, a COMPLETED token is final.
                continue;

            } else if (token.status == WFRunStatus.RUNNING) {
                // If there are no pending taskruns and the last one executed was COMPLETED,
                // then the token is now completed.
                if (token.upNext == null || token.upNext.size() == 0) {
                    if (lastTr.status == LHStatus.COMPLETED) {
                        token.status = WFRunStatus.COMPLETED;
                    }
                } else if (lastTr.status == LHStatus.ERROR) {
                    token.status = WFRunStatus.HALTED;
                }

            } else if (token.status == WFRunStatus.HALTED) {
                // This shouldn't really be possible I don't think
                LHUtil.log("What? How are we getting here when the token is already halted?");
            } else if (token.status == WFRunStatus.HALTING) {
                // Well we just gotta see if the last task run is done.
                if (lastTr.status == LHStatus.COMPLETED || lastTr.status == LHStatus.ERROR) {
                    token.status = WFRunStatus.HALTED;
                }
            }
        }

        if (wfRun.status == WFRunStatus.HALTING) {
            boolean allHalted = true;
            for (WFTokenSchema token: wfRun.tokens) {
                if (token.status == WFRunStatus.HALTING) {
                    allHalted = false;
                } else if (token.status == WFRunStatus.RUNNING) {
                    LHUtil.log("WTF how is the token RUNNING while wfRun is HALTING?");
                }
            }
            if (allHalted) {
                wfRun.status = WFRunStatus.HALTED;
            }
        } else if (wfRun.status == WFRunStatus.RUNNING) {
            boolean allCompleted = true;
            for (WFTokenSchema token: wfRun.tokens) {
                if (token.status == WFRunStatus.RUNNING) {
                    allCompleted = false;
                } else if (token.status != WFRunStatus.COMPLETED) {
                    LHUtil.log("WTF is this? Got a halted or halting token but wfrun is running");
                }
            }
            if (allCompleted) {
                wfRun.status = WFRunStatus.COMPLETED;
            }
        }
    }

    private void advanceToken(
        WFRunSchema wfRun,
        WFTokenSchema tokenToAdvance,
        WFSpec wfSpec,
        WFEventSchema wfEvent
    ) {
        if (tokenToAdvance.status != WFRunStatus.RUNNING) {
            LHUtil.log("Skipping token ", tokenToAdvance, " Not RUNNING.");
            return;
        }

        ArrayList<TaskRunSchema> nextUp = tokenToAdvance.upNext;
        if (nextUp == null || nextUp.size() == 0) {
            // Nothing to do here.

        } else if (nextUp.size() == 1) {
            // Then there's only one taskRun to schedule.
            TaskRunSchema tr = nextUp.get(0);
            NodeSchema node = wfSpec.getModel().nodes.get(tr.nodeName);

            if (node.nodeType == NodeType.TASK) {
                tr.status = LHStatus.SCHEDULED;
                tokenToAdvance.taskRuns.add(tr);
                if (node.guid.equals(actor.getNodeGuid())) {
                    actor.act(wfRun, tokenToAdvance.tokenNumber, tr.number);
                }
                // The task has been scheduled (either by this node, two lines above, or
                // by the node that is supposed to run that task). Therefore, nothing is
                // up next until the task completes or times out.
                tokenToAdvance.upNext = null;

            } else if (node.nodeType == NodeType.EXTERNAL_EVENT) {
                if (tr.startTime == null) {
                    tr.startTime = wfEvent.timestamp;
                }

                ArrayList<ExternalEventCorrelSchema> relevantEvents = wfRun.pendingEvents.get(
                    node.externalEventDefName
                );
                if (relevantEvents == null) {
                    relevantEvents = new ArrayList<ExternalEventCorrelSchema>();
                    wfRun.pendingEvents.put(node.externalEventDefName, relevantEvents);
                }

                ExternalEventCorrelSchema correlSchema = null;
                for (ExternalEventCorrelSchema candidate : relevantEvents) {
                    // In the future, we may want to add the ability to signal a specific Token
                    // rather than the whole wfRun. We would do that here.
                    if (candidate.event != null && candidate.assignedNodeGuid == null) {
                        correlSchema = candidate;
                    }
                }

                if (correlSchema != null) {
                    tr.endTime = wfEvent.timestamp;
                    tr.status = LHStatus.COMPLETED;

                    try {
                        mutateVariables(wfRun, node.variableMutations, tr, wfSpec);
                        appendActivatedNodes(wfRun, node, tokenToAdvance);
                    } catch(VarSubOrzDash exn) {
                        exn.printStackTrace();
                        raiseWorkflowProcessingError(
                            "Failed substituting variable on outgoing edge from node " + node.name,
                            wfEvent,
                            LHFailureReason.VARIABLE_LOOKUP_ERROR
                        );
                    }
                }


            }

        } else {
            // Then we gotta terminate the token and add two child tokens.
            tokenToAdvance.upNext = null; // CRUCIAL.
            LHUtil.log("TODO: Actually write the thing that splits tokens off.");
        }
    }

    private void updateTaskRunsOrEvents(
        WFEventSchema event, WFRunSchema wfRun,
        final Record<String, WFEventSchema> record, WFSpec spec
    ) {
        if (event.type == WFEventType.WF_RUN_STARTED) {
            LHUtil.log("this shouldn't be possible");

        // Task Started //// Task Started //// Task Started //// Task Started //
        } else if (event.type == WFEventType.TASK_STARTED) {
            // Find the relevant task run and mark it started.
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

            WFTokenSchema token = wfRun.tokens.get(trs.tokenNumber);
            TaskRunSchema theTask = token.taskRuns.get(trs.taskRunNumber);

            // Ok, now we have the task.
            theTask.status = LHStatus.RUNNING;
            theTask.startTime = event.timestamp;
            theTask.bashCommand = trs.bashCommand;
            theTask.stdin = trs.stdin;

        // Task Completed //// Task Completed //// Task Completed //// Task Completed //
        } else if (event.type == WFEventType.NODE_COMPLETED) {
            NodeCompletedEventSchema tre = BaseSchema.fromString(
                event.content,
                NodeCompletedEventSchema.class
            );
            WFTokenSchema token = wfRun.tokens.get(tre.tokenNumber);
            TaskRunSchema task = token.taskRuns.get(tre.taskRunNumber);
            NodeSchema node = spec.getModel().nodes.get(task.nodeName);
            if (token.upNext == null) {
                token.upNext = new ArrayList<TaskRunSchema>();
            }

            if (task.status != LHStatus.RUNNING) {
                LHUtil.log("WTF how is it possible to complete Task that hasn't started?");
                }
            if (token.upNext != null && token.upNext.size() > 0) {
                LHUtil.log("How is there something in 'up next' when a task is running?");
            }

            task.status = LHStatus.COMPLETED;
            task.endTime = event.timestamp;
            task.stderr = jsonifyIfPossible(tre.stderr);
            task.stdout = jsonifyIfPossible(tre.stdout);
            task.returnCode = tre.returncode;

            try {
                mutateVariables(wfRun, node.variableMutations, task, spec);
                appendActivatedNodes(wfRun, node, token);
            } catch(VarSubOrzDash exn) {
                exn.printStackTrace();
                raiseWorkflowProcessingError(
                    "Failed substituting variable on outgoing edge from node " + node.name,
                    event,
                    LHFailureReason.VARIABLE_LOOKUP_ERROR
                );
            }

        // EXTERNAL EVENT //// EXTERNAL EVENT //// EXTERNAL EVENT //// EXTERNAL EVENT //
        } else if (event.type == WFEventType.EXTERNAL_EVENT) {
            // Just add it to pendingEvents.
            ExternalEventPayloadSchema payload = BaseSchema.fromString(
                event.content,
                ExternalEventPayloadSchema.class
            );

            if (isSignal(payload, spec, wfRun)) {
                // This means we need to interrupt the workflow.
                // TODO: Do that later.

            } else {
                // Then just append it to pendingEvents.
                ExternalEventCorrelSchema schema = new ExternalEventCorrelSchema();
                schema.event = payload;
                if (wfRun.pendingEvents == null) {
                    wfRun.pendingEvents = new HashMap<
                        String, ArrayList<ExternalEventCorrelSchema>
                    >();
                }
                if (wfRun.pendingEvents.get(payload.externalEventDefName) == null) {
                    wfRun.pendingEvents.put(
                        payload.externalEventDefGuid,
                        new ArrayList<ExternalEventCorrelSchema>()
                    );
                }
                wfRun.pendingEvents.get(payload.externalEventDefName).add(schema);

            }

        } else if (event.type == WFEventType.WORKFLOW_PROCESSING_FAILED) {
            LHUtil.log("here we are in wfprocessing failed");
        } else if (event.type == WFEventType.TASK_FAILED) {
            TaskRunFailedEventSchema trf = BaseSchema.fromString(
                event.content, TaskRunFailedEventSchema.class
            );

            WFTokenSchema token = wfRun.tokens.get(trf.tokenNumber);
            TaskRunSchema tr = token.taskRuns.get(trf.taskRunNumber);

            if (token.upNext != null && token.upNext.size() > 0) {
                LHUtil.log("How is there something in 'up next' when a task is running?");
            }

            // When we do automatic retries, this is where we handle that.
            token.status = WFRunStatus.HALTED;

            tr.returnCode = trf.returncode;
            tr.endTime = event.timestamp;
            tr.failureMessage = trf.message;
            tr.failureReason = trf.reason;
            tr.stdout = jsonifyIfPossible(trf.stdout);
            tr.stderr = jsonifyIfPossible(trf.stderr);
            tr.status = LHStatus.ERROR;
        }
    }
    
    private boolean isSignal(ExternalEventPayloadSchema payload, WFSpec spec, WFRunSchema wfRun) {
        // TODO: when we support interrupts, this will have to do something smart.
        return false;
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

    private boolean shouldHalt(WFRunSchema wfRun, WFEventSchema event) {
        // In the future there may be some more things added to the WFSpec which says "certain
        // tasks can fail without stopping the whole world" or "if this token fails, kill the
        // whole workflow, but if that token fails, just let the other ones keep going."
        // That logic should live here.
        return (
            event.type == WFEventType.WORKFLOW_PROCESSING_FAILED ||
            event.type == WFEventType.TASK_FAILED
        );
    }

    private boolean shouldStart(WFRunSchema wfRun, WFEventSchema event) {
        // As of now, we don't have any "please resume" events, like manual restarts or
        // things like that.
        return false;
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

    private void appendActivatedNodes(
        WFRunSchema wfRun, NodeSchema node, WFTokenSchema token
    ) throws VarSubOrzDash {
        WFSpec spec = getWFSpec(wfRun.wfSpecGuid);
        for (EdgeSchema edge : node.outgoingEdges) {
            if (WFRun.evaluateEdge(wfRun, edge.condition, token)) {
                TaskRunSchema task = new TaskRunSchema();
                NodeSchema newNode = spec.getModel().nodes.get(edge.sinkNodeName);
                task.status = LHStatus.PENDING;
                task.nodeGuid = newNode.guid;
                task.nodeName = newNode.name;
                task.wfSpecGuid = wfRun.wfSpecGuid;
                task.wfSpecName = wfRun.wfSpecName;
                task.number = token.taskRuns.size();
                task.tokenNumber = token.tokenNumber;

                token.upNext.add(task);
            }
        }
    }

    private Object jsonifyIfPossible(String data) {
        try {
            Object obj = LHUtil.mapper.readValue(data, Object.class);
            return obj;
        } catch(Exception exn) {
            return data;
        }
    }

    public WFRunSchema handleWFRunStarted(
        WFEventSchema event,
        WFRunSchema wfRun,
        final Record<String, WFEventSchema> record,
        WFSpec wfSpec
    ) {
        WFRunRequestSchema runRequest = BaseSchema.fromString(
            event.content, WFRunRequestSchema.class
        );

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
        wfRun.status = WFRunStatus.RUNNING;
        wfRun.tokens = new ArrayList<WFTokenSchema>();
        
        WFTokenSchema token = new WFTokenSchema();
        token.tokenNumber = 0;
        token.status = WFRunStatus.RUNNING;
        token.taskRuns = new ArrayList<TaskRunSchema>();

        WFSpecSchema wfSpecSchema = wfSpec.getModel();
        NodeSchema node = wfSpecSchema.nodes.get(
            wfSpecSchema.entrypointNodeName
        );
        TaskRunSchema tr = new TaskRunSchema();
        tr.status = LHStatus.PENDING;
        tr.tokenNumber = 0;
        tr.number = 0;
        tr.nodeGuid = node.guid;
        tr.nodeName = node.name;
        // token.taskRuns.add(tr);
        token.upNext = new ArrayList<TaskRunSchema>();
        token.upNext.add(tr);

        wfRun.tokens.add(token);
        
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

        return wfRun;
    }
}
