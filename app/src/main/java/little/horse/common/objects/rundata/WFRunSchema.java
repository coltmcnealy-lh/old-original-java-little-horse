package little.horse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import little.horse.common.Config;
import little.horse.common.events.ExternalEventCorrelSchema;
import little.horse.common.events.ExternalEventPayloadSchema;
import little.horse.common.events.WFEventIDSchema;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.EdgeSchema;
import little.horse.common.objects.metadata.NodeSchema;
import little.horse.common.objects.metadata.ThreadSpecSchema;
import little.horse.common.objects.metadata.VariableAssignmentSchema;
import little.horse.common.objects.metadata.VariableMutationSchema;
import little.horse.common.objects.metadata.WFRunVariableDefSchema;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;

public class WFRunSchema extends BaseSchema {
    // These fields are in the actual JSON for the WFRunSchema object
    public String guid;
    public String wfSpecGuid;
    public String wfSpecName;

    @JsonManagedReference
    public ArrayList<ThreadRunSchema> threadRuns;

    public WFRunStatus status;
    public Date startTime;
    public Date endTime;

    public LHFailureReason errorCode;
    public String errorMessage;

    public ArrayList<WFEventIDSchema> history;  // Event Sourcing! Yay!

    public HashMap<String, ArrayList<ExternalEventCorrelSchema>> correlatedEvents;
    public Stack<String> pendingInterrupts;

    public HashMap<String, ArrayList<ThreadRunMetaSchema>> awaitableThreads;

    @JsonIgnore
    private WFSpecSchema wfSpec;

    @JsonIgnore
    public void setWFSpec(WFSpecSchema spec) {
        wfSpec = spec;
    }

    @JsonIgnore
    public WFSpecSchema getWFSpec() throws LHConnectionError {
        if (wfSpec == null) {
            String id = (wfSpecGuid == null) ? wfSpecName : wfSpecGuid;
            wfSpec = LHDatabaseClient.lookupWFSpec(id, config);
        }
        return wfSpec;
    }

    @JsonIgnore
    public ThreadRunSchema createThreadClientAdds(
        String threadName, Map<String, Object> variables, ThreadRunSchema parent
    ) throws LHConnectionError {
        getWFSpec();  // just make sure the thing isn't null;

        // Since the wfSpec has already been validated (at the time it was created)
        // via the API, this is supposedly hypotentially in theory guaranteed to
        // return a ThreadSpecSchema (otherwise there would've been an error thrown
        // at WFSpec creation time).
        ThreadSpecSchema tspec = wfSpec.threadSpecs.get(threadName);
        // TODO: do the fillOut() and linkUp() here.

        ThreadRunSchema trun = new ThreadRunSchema();
        setConfig(config); // this will populate the ThreadRun as well

        trun.id = threadRuns.size();
        trun.status = parent == null ? WFRunStatus.RUNNING : parent.status;
        trun.taskRuns = new ArrayList<TaskRunSchema>();

        // Load the variables for the ThreadRun
        trun.variables = new HashMap<String, Object>();
        for (String varName: tspec.variableDefs.keySet()) {
            WFRunVariableDefSchema varDef = tspec.variableDefs.get(varName);

            Object result = variables.get(varName);
            if (result != null) {
                trun.variables.put(varName, result);
            } else {
                trun.variables.put(varName, varDef.defaultValue);
            }
        }
        trun.upNext = new ArrayList<EdgeSchema>();
        trun.threadSpec = wfSpec.threadSpecs.get(threadName);
        trun.threadSpecName = threadName;
        // trun.threadSpecGuid = trun.threadSpec.guid;
        throw new RuntimeException("Oops");
        // trun.errorMessage = "";

        // trun.activeInterruptThreadIDs = new ArrayList<Integer>();
        // trun.handledInterruptThreadIDs = new ArrayList<Integer>();

        // // Now add the entrypoint taskRun
        // EdgeSchema fakeEdge = new EdgeSchema();
        // fakeEdge.sinkNodeName = tspec.entrypointNodeName;
        // trun.addEdgeToUpNext(fakeEdge);

        // trun.childThreadIDs = new ArrayList<>();
        // trun.wfRun = this;
        // trun.variableLocks = new HashMap<String, Integer>();

        // trun.completedExeptionHandlerThreads = new ArrayList<Integer>();

        // trun.haltReasons = new HashSet<>();

        // if (parent != null) {
        //     parent.childThreadIDs.add(trun.id);
        //     trun.parentThreadID = parent.id;

        //     if (parent.status == WFRunStatus.HALTED ||
        //         parent.status == WFRunStatus.HALTING
        //     ) {
        //         trun.haltReasons.add(WFHaltReasonEnum.PARENT_STOPPED);
        //     }
        // }

        // return trun;
    }

    @JsonIgnore
    public WFEventSchema newWFEvent(WFEventType type, BaseSchema content) {
        WFEventSchema event = new WFEventSchema();
        event.setConfig(config);
        event.type = type;
        event.wfRunGuid = guid;
        event.wfSpecGuid = wfSpecGuid;
        event.wfSpecName = wfSpecName;
        event.timestamp = LHUtil.now();
        event.content = content.toString();
        event.wfRun = this;
        return event;
    }

    @JsonIgnore
    private void handleExternalEvent(WFEventSchema event) throws
    LHConnectionError {
        ExternalEventPayloadSchema payload = BaseSchema.fromString(
            event.content, ExternalEventPayloadSchema.class, config, false
        );
        
        if (wfSpec.interruptEvents.contains(payload.externalEventDefName)) {
            // This is an interrupt. Find the appropriate thread and interrupt it.
            // There's two options: the thread number is set, or it's not set.
            // If the thread number is set, then just interrupt that thread.
            if (event.threadID != -1) {
                threadRuns.get(event.threadID).handleInterrupt(payload);
            } else {
                // if there's no thread number set, interrupt all threads that
                // listen to this interrupt.
                threadRuns.get(0).propagateInterrupt(payload);
            }
        } else {
            // This isn't an interrupt, so store the event in case some other
            // thread has an EXTERNAL_EVENT node that waits for this type of event.
            ExternalEventCorrelSchema correl = new ExternalEventCorrelSchema();
            correl.event = payload;
            correl.arrivalTime = event.timestamp;
            correl.assignedThreadID = event.threadID;

            if (correlatedEvents == null) {
                correlatedEvents = new HashMap<>();
            }
            if (correlatedEvents.get(payload.externalEventDefName) == null) {
                correlatedEvents.put(
                    payload.externalEventDefName, new ArrayList<>()
                );
            }

            correlatedEvents.get(payload.externalEventDefName).add(correl);
        }
    }

    @JsonIgnore
    public void incorporateEvent(WFEventSchema event)
    throws LHConnectionError {
        if (event.type == WFEventType.WF_RUN_STARTED) {
            throw new RuntimeException(
                "This shouldn't happen, colty you're programming like you're drunk"
            );
        }

        if (event.type == WFEventType.EXTERNAL_EVENT) {
            handleExternalEvent(event);
            return;
        }

        if (event.type == WFEventType.TASK_EVENT) {
            ThreadRunSchema thread = threadRuns.get(event.threadID);
            thread.incorporateEvent(event);
        }

        if (event.type == WFEventType.WF_RUN_STOP_REQUEST) {
            if (event.threadID == 0 && status == WFRunStatus.RUNNING) {
                status = WFRunStatus.HALTING;
            }
            int threadID = event.threadID >= 0 ? event.threadID : 0;
            if (threadID < threadRuns.size()) {
                threadRuns.get(event.threadID).halt(
                    WFHaltReasonEnum.MANUAL_STOP,
                    "Manual halt of this thread requested by system admin."
                );
            }
        }

        if (event.type == WFEventType.WF_RUN_RESUME_REQUEST) {
            if (event.threadID == 0 && status != WFRunStatus.COMPLETED) {
                status = WFRunStatus.RUNNING;
            }
            if (event.threadID < threadRuns.size()) {
                threadRuns.get(event.threadID).removeHaltReason(
                    WFHaltReasonEnum.MANUAL_STOP
                );
            }
        }
    }

    @JsonIgnore
    public void updateStatuses(WFEventSchema event) {
        for (ThreadRunSchema thread: threadRuns) {
            thread.updateStatus();
        }

        // // lmfao but this do be necessary tho
        // for (int i = threadRuns.size() - 1; i >= 0; i--) {
        //     threadRuns.get(i).updateStatus();
        // }

        boolean allTerminated = true;
        boolean allCompleted = true;

        if (status == WFRunStatus.HALTING) {
            boolean allHalted = true;
            for (ThreadRunSchema thread: this.threadRuns) {
                if (thread.status == WFRunStatus.HALTING) {
                    allHalted = false;
                } else if (thread.status == WFRunStatus.RUNNING) {
                    LHUtil.log("WTF how is the thread RUNNING while wfRun is HALTING?");
                }
            }
            if (allHalted) {
                this.status = WFRunStatus.HALTED;
            }
        } else if (this.status == WFRunStatus.RUNNING) {
            for (ThreadRunSchema thread: this.threadRuns) {
                if (!thread.isTerminated()) allTerminated = false;
                if (!thread.isCompleted()) allCompleted = false;
            }
            if (allCompleted) {
                this.status = WFRunStatus.COMPLETED;
            } else if (allTerminated) {
                this.status = WFRunStatus.HALTED;
            }
        }
    }

    public static HashSet<String> getNeededVars(NodeSchema n) {
        HashSet<String> neededVars = new HashSet<String>();
        // first figure out which variables we need as input
        for (VariableAssignmentSchema var: n.variables.values()) {
            if (var.wfRunVariableName != null) {
                neededVars.add(var.wfRunVariableName);
            }
        }

        // Now see which variables we need as output
        for (Map.Entry<String, VariableMutationSchema> p:
            n.variableMutations.entrySet()
        ) {
            // Add the variable that gets mutated
            neededVars.add(p.getKey());

            VariableAssignmentSchema rhsVarAssign = p.getValue().sourceVariable;
            if (rhsVarAssign != null) {
                if (rhsVarAssign.wfRunVariableName != null) {
                    neededVars.add(rhsVarAssign.wfRunVariableName);
                }
            }
        }
        return neededVars;
    }

    @JsonIgnore
    public ThreadRunSchema entrypointThreadRun() {
        return threadRuns.get(0);
    }

    @Override
    @JsonIgnore
    public Config setConfig(Config config) {
        super.setConfig(config);
        if (threadRuns == null) threadRuns = new ArrayList<>();
        for (ThreadRunSchema thread: threadRuns) {
            thread.wfRun = this;
            thread.setConfig(config);
        }
        return this.config;
    }
}
