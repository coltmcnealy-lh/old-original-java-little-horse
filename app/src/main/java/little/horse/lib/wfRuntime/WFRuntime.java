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
import little.horse.lib.LHDatabaseClient;
import little.horse.lib.LHFailureReason;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
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
import little.horse.lib.schemas.TaskRunEndedEventSchema;
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
import little.horse.lib.schemas.ThreadRunSchema;
import little.horse.lib.schemas.ThreadSpecSchema;


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

        updateStatuses(wfRun);
        HashSet<String> alreadySeen = new HashSet<String>();
        ThreadRunSchema threadToAdvance = getThreadToAdvance(wfRun, alreadySeen);
        while (threadToAdvance != null) {
            advanceThread(wfRun, threadToAdvance, wfSpec, event);
            updateStatuses(wfRun);
            threadToAdvance = getThreadToAdvance(wfRun, alreadySeen);
        }

        kvStore.put(wfRun.guid, wfRun);
    }

    public static ThreadRunSchema getThreadToAdvance(WFRunSchema wfRun, Set<String> alreadySeen) {
        for (ThreadRunSchema thread: wfRun.threadRuns) {
            String key = String.valueOf(thread.id) + "__" + String.valueOf(
                thread.taskRuns.size()
            );
            if (!alreadySeen.contains(key)) {
                alreadySeen.add(key);
                return thread;
            }
        }

        return null;
    }

    private void updateStatuses(WFRunSchema wfRun) {

        for (ThreadRunSchema thread: wfRun.threadRuns) {
            if (thread.taskRuns.size() == 0) {
                LHUtil.log("WTF?");
                continue;
            }
            if (thread.upNext == null) {
                thread.upNext = new ArrayList<TaskRunSchema>();
            }
            TaskRunSchema lastTr = thread.taskRuns.get(thread.taskRuns.size() - 1);
            if (thread.status == WFRunStatus.COMPLETED) {
                // As of now, a COMPLETED thread is final.
                continue;

            } else if (thread.status == WFRunStatus.RUNNING) {
                // If there are no pending taskruns and the last one executed was COMPLETED,
                // then the thread is now completed.
                if (thread.upNext == null || thread.upNext.size() == 0) {
                    if (lastTr.status == LHStatus.COMPLETED) {
                        thread.status = WFRunStatus.COMPLETED;
                    }
                } else if (lastTr.status == LHStatus.ERROR) {
                    thread.status = WFRunStatus.HALTED;
                }

            } else if (thread.status == WFRunStatus.HALTED) {
                // This shouldn't really be possible I don't think
                LHUtil.log("What? How are we getting here when the thread is already halted?");
            } else if (thread.status == WFRunStatus.HALTING) {
                // Well we just gotta see if the last task run is done.
                if (lastTr.status == LHStatus.COMPLETED || lastTr.status == LHStatus.ERROR) {
                    thread.status = WFRunStatus.HALTED;
                }
            }
        }

        if (wfRun.status == WFRunStatus.HALTING) {
            boolean allHalted = true;
            for (ThreadRunSchema thread: wfRun.threadRuns) {
                if (thread.status == WFRunStatus.HALTING) {
                    allHalted = false;
                } else if (thread.status == WFRunStatus.RUNNING) {
                    LHUtil.log("WTF how is the thread RUNNING while wfRun is HALTING?");
                }
            }
            if (allHalted) {
                wfRun.status = WFRunStatus.HALTED;
            }
        } else if (wfRun.status == WFRunStatus.RUNNING) {
            boolean allCompleted = true;
            for (ThreadRunSchema thread: wfRun.threadRuns) {
                if (thread.status == WFRunStatus.RUNNING) {
                    allCompleted = false;
                } else if (thread.status != WFRunStatus.COMPLETED) {
                    LHUtil.log("WTF is this? Got a halted or halting thread but wfrun is running");
                }
            }
            if (allCompleted) {
                wfRun.status = WFRunStatus.COMPLETED;
            }
        }
    }

    private void advanceThread(
        WFRunSchema wfRun,
        ThreadRunSchema thread,
        WFSpec wfSpec,
        WFEventSchema wfEvent
    ) {
        if (thread.status != WFRunStatus.RUNNING) {
            return;
        }

        ThreadSpecSchema threadSpec = wfSpec.getModel().threadSpecs.get(
            thread.threadSpecName
        );

        ArrayList<TaskRunSchema> nextUp = thread.upNext;
        if (nextUp == null || nextUp.size() == 0) {
            // Nothing to do here.

        } else if (nextUp.size() == 1) {
            // Then there's only one taskRun to schedule.
            TaskRunSchema tr = nextUp.get(0);
            NodeSchema node = threadSpec.nodes.get(tr.nodeName);

            if (node.nodeType == NodeType.TASK) {
                tr.status = LHStatus.SCHEDULED;
                thread.taskRuns.add(tr);
                if (node.guid.equals(actor.getNodeGuid())) {
                    actor.act(wfRun, thread.id, tr.number);
                }
                // The task has been scheduled (either by this node, two lines above, or
                // by the node that is supposed to run that task). Therefore, nothing is
                // up next until the task completes or times out.
                thread.upNext = null;

            } else if (node.nodeType == NodeType.EXTERNAL_EVENT) {
                if (tr.startTime == null) {
                    tr.startTime = wfEvent.timestamp;
                }

                ArrayList<ExternalEventCorrelSchema> relevantEvents = wfRun.correlatedEvents.get(
                    node.externalEventDefName
                );
                if (relevantEvents == null) {
                    relevantEvents = new ArrayList<ExternalEventCorrelSchema>();
                    wfRun.correlatedEvents.put(node.externalEventDefName, relevantEvents);
                }

                ExternalEventCorrelSchema correlSchema = null;
                for (ExternalEventCorrelSchema candidate : relevantEvents) {
                    // In the future, we may want to add the ability to signal a specific thread
                    // rather than the whole wfRun. We would do that here.
                    if (candidate.event != null && candidate.assignedNodeGuid == null) {
                        correlSchema = candidate;
                    }
                }

                if (correlSchema != null) {
                    tr.endTime = wfEvent.timestamp;
                    tr.status = LHStatus.COMPLETED;
                    tr.stdout = jsonifyIfPossible(wfEvent.content);
                    correlSchema.assignedNodeGuid = node.guid;
                    correlSchema.assignedNodeName = node.name;
                    correlSchema.assignedTaskRunExecutionNumber = tr.number;
                    correlSchema.assignedThreadID = tr.threadID;

                    try {
                        mutateVariables(wfRun, node.variableMutations, tr, wfSpec);
                        thread.taskRuns.add(tr);
                        thread.upNext = new ArrayList<TaskRunSchema>();
                        appendActivatedNodes(wfRun, node, thread);
                    } catch(VarSubOrzDash exn) {
                        exn.printStackTrace();
                        raiseWorkflowProcessingError(
                            "Failed substituting variable on outgoing edge from node " + node.name,
                            wfEvent,
                            LHFailureReason.VARIABLE_LOOKUP_ERROR
                        );
                    }
                }
            } else if (node.nodeType == NodeType.SPAWN_THREAD) {

            } else if (node.nodeType == NodeType.WAIT_FOR_THREAD) {

            }

        } else {
            // Then there are more than one nodes up next. This should not be possible, since
            // you should only be able to split a thread off via a specific call to SPAWN_THREAD.
            thread.upNext = null; // CRUCIAL.
            thread.status = WFRunStatus.HALTED;
            LHUtil.log("This is a problem: There were two outgoing edges.");
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

            ThreadRunSchema thread = wfRun.threadRuns.get(trs.threadID);
            TaskRunSchema theTask = thread.taskRuns.get(trs.taskRunNumber);

            // Ok, now we have the task.
            theTask.status = LHStatus.RUNNING;
            theTask.startTime = event.timestamp;
            theTask.bashCommand = trs.bashCommand;
            theTask.stdin = trs.stdin;

        // Task Completed //// Task Completed //// Task Completed //// Task Completed //
        } else if (event.type == WFEventType.TASK_COMPLETED) {
            TaskRunEndedEventSchema tre = BaseSchema.fromString(
                event.content,
                TaskRunEndedEventSchema.class
            );
            ThreadRunSchema thread = wfRun.threadRuns.get(tre.threadID);
            ThreadSpecSchema threadSpec = spec.getModel().threadSpecs.get(
                thread.threadSpecName
            );
            TaskRunSchema task = thread.taskRuns.get(tre.taskRunNumber);
            NodeSchema node = threadSpec.nodes.get(task.nodeName);
            if (thread.upNext == null) {
                thread.upNext = new ArrayList<TaskRunSchema>();
            }

            if (task.status != LHStatus.RUNNING) {
                LHUtil.log("WTF how is it possible to complete Task that hasn't started?");
                }
            if (thread.upNext != null && thread.upNext.size() > 0) {
                LHUtil.log("How is there something in 'up next' when a task is running?");
            }

            task.status = LHStatus.COMPLETED;
            task.endTime = event.timestamp;
            task.stderr = jsonifyIfPossible(tre.stderr);
            task.stdout = jsonifyIfPossible(tre.stdout);
            task.returnCode = tre.returncode;

            try {
                mutateVariables(wfRun, node.variableMutations, task, spec);
                appendActivatedNodes(wfRun, node, thread);
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
                if (wfRun.correlatedEvents == null) {
                    wfRun.correlatedEvents = new HashMap<
                        String, ArrayList<ExternalEventCorrelSchema>
                    >();
                }
                if (wfRun.correlatedEvents.get(payload.externalEventDefName) == null) {
                    wfRun.correlatedEvents.put(
                        payload.externalEventDefGuid,
                        new ArrayList<ExternalEventCorrelSchema>()
                    );
                }
                wfRun.correlatedEvents.get(payload.externalEventDefName).add(schema);

            }

        } else if (event.type == WFEventType.WORKFLOW_PROCESSING_FAILED) {
            LHUtil.log("here we are in wfprocessing failed");
        } else if (event.type == WFEventType.TASK_FAILED) {
            TaskRunFailedEventSchema trf = BaseSchema.fromString(
                event.content, TaskRunFailedEventSchema.class
            );

            ThreadRunSchema thread = wfRun.threadRuns.get(trf.threadID);
            TaskRunSchema tr = thread.taskRuns.get(trf.taskRunNumber);

            if (thread.upNext != null && thread.upNext.size() > 0) {
                LHUtil.log("How is there something in 'up next' when a task is running?");
            }

            // When we do automatic retries, this is where we handle that.
            thread.status = WFRunStatus.HALTED;

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

    private WFSpecSchema getWFSpec(String guid) throws LHLookupException, LHNoConfigException {
        if (wfspecs.get(guid) != null) {
            return wfspecs.get(guid);
        }
        // TODO: Do some caching here—that's the only reason we have this.
        WFSpecSchema result = LHDatabaseClient.lookupWFSpec(guid, config);
        wfspecs.put(guid, result);
        return result;
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
        // tasks can fail without stopping the whole world" or "if this thread fails, kill the
        // whole workflow, but if that thread fails, just let the other ones keep going."
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
        WFRunSchema wfRun, NodeSchema node, ThreadRunSchema thread
    ) throws VarSubOrzDash {
        WFSpec spec = getWFSpec(wfRun.wfSpecGuid);
        ThreadSpecSchema threadSpec = spec.getModel().threadSpecs.get(
            thread.threadSpecName
        );
        for (EdgeSchema edge : node.outgoingEdges) {
            if (WFRun.evaluateEdge(wfRun, edge.condition, thread)) {
                TaskRunSchema task = new TaskRunSchema();
                NodeSchema newNode = threadSpec.nodes.get(edge.sinkNodeName);
                task.status = LHStatus.PENDING;
                task.nodeGuid = newNode.guid;
                task.nodeName = newNode.name;
                task.wfSpecGuid = wfRun.wfSpecGuid;
                task.wfSpecName = wfRun.wfSpecName;
                task.number = thread.taskRuns.size();
                task.threadID = thread.id;

                thread.upNext.add(task);
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

}
