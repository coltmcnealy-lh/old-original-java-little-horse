package little.horse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import io.javalin.Javalin;
import io.javalin.http.Context;
import little.horse.api.ResponseStatus;
import little.horse.common.Config;
import little.horse.common.events.ExternalEventCorrel;
import little.horse.common.events.ExternalEventPayload;
import little.horse.common.events.WFEventID;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFEventType;
import little.horse.common.events.WFRunRequest;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.metadata.Edge;
import little.horse.common.objects.metadata.ExternalEventDef;
import little.horse.common.objects.metadata.ThreadSpec;
import little.horse.common.objects.metadata.WFRunVariableDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHRpcResponse;
import little.horse.common.util.LHUtil;

@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "guid"
)
public class WFRun extends CoreMetadata {
    // These fields are in the actual JSON for the WFRunSchema object
    public String id;

    @Override
    public String getId() {
        return id;
    }

    public String wfSpecDigest;
    public String wfSpecName;

    @JsonManagedReference
    public ArrayList<ThreadRun> threadRuns;

    public LHExecutionStatus status;
    public Date startTime;
    public Date endTime;

    public LHFailureReason errorCode;
    public String errorMessage;

    public ArrayList<WFEventID> history;  // Event Sourcing! Yay!

    public HashMap<String, ArrayList<ExternalEventCorrel>> correlatedEvents;
    public Stack<String> pendingInterrupts;

    public HashMap<String, ArrayList<ThreadRunMeta>> awaitableThreads;

    @JsonIgnore
    private WFSpec wfSpec;

    @JsonIgnore
    public void setWFSpec(WFSpec spec) {
        wfSpec = spec;
        wfSpecDigest = wfSpec.getId();
        wfSpecName = wfSpec.name;
    }

    @JsonIgnore
    public WFSpec getWFSpec() throws LHConnectionError {
        if (wfSpec == null) {
            String id = (wfSpecDigest == null) ? wfSpecName : wfSpecDigest;
            setWFSpec(LHDatabaseClient.lookupMeta(id, config, WFSpec.class));
        }
        return wfSpec;
    }

    @JsonIgnore
    public ThreadRun createThreadClientAdds(
        String threadName, Map<String, Object> variables, ThreadRun parent
    ) throws LHConnectionError {
        getWFSpec();  // just make sure the thing isn't null;

        // Since the wfSpec has already been validated (at the time it was created)
        // via the API, this is supposedly hypotentially in theory guaranteed to
        // return a ThreadSpecSchema (otherwise there would've been an error thrown
        // at WFSpec creation time).
        ThreadSpec tspec = wfSpec.threadSpecs.get(threadName);
        // TODO: do the fillOut() and linkUp() here.

        ThreadRun trun = new ThreadRun();
        setConfig(config); // this will populate the ThreadRun as well

        trun.id = threadRuns.size();
        trun.status = parent == null ? LHExecutionStatus.RUNNING : parent.status;
        trun.taskRuns = new ArrayList<TaskRun>();

        // Load the variables for the ThreadRun
        trun.variables = new HashMap<String, Object>();
        for (String varName: tspec.variableDefs.keySet()) {
            WFRunVariableDef varDef = tspec.variableDefs.get(varName);

            Object result = variables.get(varName);
            if (result != null) {
                trun.variables.put(varName, result);
            } else {
                trun.variables.put(varName, varDef.defaultValue);
            }
        }
        trun.upNext = new ArrayList<Edge>();
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
    public WFEvent newWFEvent(WFEventType type, BaseSchema content) {
        WFEvent event = new WFEvent();
        event.setConfig(config);
        event.type = type;
        event.wfRunId = id;
        event.wfSpecDigest = wfSpecDigest;
        event.wfSpecName = wfSpecName;
        event.timestamp = LHUtil.now();
        event.content = content.toString();
        event.wfRun = this;
        return event;
    }

    @JsonIgnore
    private void handleExternalEvent(WFEvent event) throws
    LHConnectionError {
        ExternalEventPayload payload;
        try {
            payload = BaseSchema.fromString(
                event.content, ExternalEventPayload.class, config
            );
        } catch(LHSerdeError exn) {
            exn.printStackTrace();
            LHUtil.logError("Ignoring exception since there's nothing we can do.");
            return;
        }
        
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
            ExternalEventCorrel correl = new ExternalEventCorrel();
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
    public void incorporateEvent(WFEvent event)
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
            ThreadRun thread = threadRuns.get(event.threadID);
            thread.incorporateEvent(event);
        }

        if (event.type == WFEventType.WF_RUN_STOP_REQUEST) {
            if (event.threadID == 0 && status == LHExecutionStatus.RUNNING) {
                status = LHExecutionStatus.HALTING;
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
            if (event.threadID == 0 && status != LHExecutionStatus.COMPLETED) {
                status = LHExecutionStatus.RUNNING;
            }
            if (event.threadID < threadRuns.size()) {
                threadRuns.get(event.threadID).removeHaltReason(
                    WFHaltReasonEnum.MANUAL_STOP
                );
            }
        }
    }

    @JsonIgnore
    public void updateStatuses(WFEvent event) {
        for (ThreadRun thread: threadRuns) {
            thread.updateStatus();
        }

        // // lmfao but this do be necessary tho
        // for (int i = threadRuns.size() - 1; i >= 0; i--) {
        //     threadRuns.get(i).updateStatus();
        // }

        boolean allTerminated = true;
        boolean allCompleted = true;

        if (status == LHExecutionStatus.HALTING) {
            boolean allHalted = true;
            for (ThreadRun thread: this.threadRuns) {
                if (thread.status == LHExecutionStatus.HALTING) {
                    allHalted = false;
                } else if (thread.status == LHExecutionStatus.RUNNING) {
                    LHUtil.log("WTF how is the thread RUNNING while wfRun is HALTING?");
                }
            }
            if (allHalted) {
                this.status = LHExecutionStatus.HALTED;
            }
        } else if (this.status == LHExecutionStatus.RUNNING) {
            for (ThreadRun thread: this.threadRuns) {
                if (!thread.isTerminated()) allTerminated = false;
                if (!thread.isCompleted()) allCompleted = false;
            }
            if (allCompleted) {
                this.status = LHExecutionStatus.COMPLETED;
            } else if (allTerminated) {
                this.status = LHExecutionStatus.HALTED;
            }
        }
    }

    // public static HashSet<String> getNeededVars(NodeSchema n) {
    //     HashSet<String> neededVars = new HashSet<String>();
    //     // first figure out which variables we need as input
    //     for (VariableAssignmentSchema var: n.variables.values()) {
    //         if (var.wfRunVariableName != null) {
    //             neededVars.add(var.wfRunVariableName);
    //         }
    //     }

    //     // Now see which variables we need as output
    //     for (Map.Entry<String, VariableMutationSchema> p:
    //         n.variableMutations.entrySet()
    //     ) {
    //         // Add the variable that gets mutated
    //         neededVars.add(p.getKey());

    //         VariableAssignmentSchema rhsVarAssign = p.getValue().sourceVariable;
    //         if (rhsVarAssign != null) {
    //             if (rhsVarAssign.wfRunVariableName != null) {
    //                 neededVars.add(rhsVarAssign.wfRunVariableName);
    //             }
    //         }
    //     }
    //     return neededVars;
    // }

    @JsonIgnore
    public ThreadRun entrypointThreadRun() {
        return threadRuns.get(0);
    }

    // Override the next two methods for the sake of compatibility with CoreMetadata,
    // even though this isn't truly CoreMetadata.
    @Override
    public void validate(Config config) {}

    @Override
    public void processChange(CoreMetadata old) {}

    @JsonIgnore
    public static boolean onlyUseDefaultAPIforGET = false;

    private WFRunApiStuff apiStuff;;

    @Override
    public void overridePostAPIEndpoints(Javalin app) {
        apiStuff = new WFRunApiStuff(config);

        app.post("/wfRun", apiStuff::postRun);
        app.post(
            "/externalEvent/{externalEventDefId}/{wfRunId}", apiStuff::postEvent
        );

        app.post("/wfRun/stop/{wfRunId}/{tid}", apiStuff::postStopThread);
        app.post("/wfRun/resume/{wfRunId}/{tid}", apiStuff::postResumeThread);


    }

}

class WFRunApiStuff {
    private Config config;

    public WFRunApiStuff(Config config) {
        this.config = config;
    }

    public void postRun(Context ctx) {
        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        WFRunRequest request = ctx.bodyAsClass(WFRunRequest.class);
        WFEvent event = new WFEvent();
        event.setConfig(config);
        String guid = LHUtil.generateGuid();
        event.wfRunId = guid;
        event.content = request.toString();
        event.type = WFEventType.WF_RUN_STARTED;

        response.objectId = guid;

        try {
            event.record();
            response.status = ResponseStatus.OK;
        } catch(LHConnectionError exn) {
            response.status = ResponseStatus.INTERNAL_ERROR;
            response.message = exn.getMessage();
            ctx.json(response);
            return;
        }

        ctx.json(response);
        return;
    }

    public void postStopThread(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        int tid = Integer.valueOf(ctx.pathParam("tid"));

        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        try {

            WFRun wfRun = LHDatabaseClient.lookupMeta(wfRunGuid, config, WFRun.class);
            if (wfRun == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "Could not find provided wfRun with provided id.";
                ctx.json(response);
                return;
            }

            WFEvent event = new WFEvent();
            event.setConfig(config);
            event.wfRun = wfRun;
            event.wfRunId = wfRunGuid;
            event.type = WFEventType.WF_RUN_STOP_REQUEST;    
            event.threadID = tid;
            event.wfSpecDigest = event.wfRun.wfSpecDigest;
            event.wfSpecName = event.wfRun.wfSpecName;
            event.record();

            response.result = wfRun;

            ctx.json(response);

        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            ctx.status(500);
            response.message = "Couldn't stop the wfRun: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.json(response);
            return;
        }
    }

    public void postResumeThread(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        int tid = Integer.valueOf(ctx.pathParam("tid"));

        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        try {

            WFRun wfRun = LHDatabaseClient.lookupMeta(wfRunGuid, config, WFRun.class);
            if (wfRun == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "Could not find provided wfRun with provided id.";
                ctx.json(response);
                return;
            }

            WFEvent event = new WFEvent();
            event.setConfig(config);
            event.wfRun = wfRun;
            event.wfRunId = wfRunGuid;
            event.type = WFEventType.WF_RUN_RESUME_REQUEST;    
            event.threadID = tid;
            event.wfSpecDigest = event.wfRun.wfSpecDigest;
            event.wfSpecName = event.wfRun.wfSpecName;
            event.record();

            response.result = wfRun;

            ctx.json(response);

        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            ctx.status(500);
            response.message = "Couldn't resume the wfRun: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.json(response);
            return;
        }
    }

    public void postEvent(Context ctx) {
        String wfRunId = ctx.pathParam("wfRunId");
        String externalEventDefID = ctx.pathParam("externalEventDefID");
        Object eventContent = ctx.bodyAsClass(Object.class);
        WFRun wfRun;
        ExternalEventDef evd;

        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        try {
            wfRun = LHDatabaseClient.lookupMeta(wfRunId, config, WFRun.class);
            if (wfRun == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "Couldn't find wfRun with id " + wfRunId;
                ctx.json(response);
                return;
            }

            evd = LHDatabaseClient.lookupMeta(
                externalEventDefID, config, ExternalEventDef.class
            );

            if (evd == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "Couldn't find Ext Ev Def with id "
                    + externalEventDefID;
                ctx.json(response);
                return;
            }

            ExternalEventPayload payload = new ExternalEventPayload();
            payload.externalEventDefId = evd.getId();
            payload.externalEventDefName = evd.name;
            payload.content = eventContent;

            WFEvent wfEvent = new WFEvent();
            wfEvent.wfRunId = wfRun.getId();
            wfEvent.wfSpecDigest = wfRun.wfSpecDigest;
            wfEvent.wfSpecName = wfRun.wfSpecName;
            wfEvent.type = WFEventType.EXTERNAL_EVENT;
            wfEvent.timestamp = LHUtil.now();
            wfEvent.content = payload.toString();
            wfEvent.wfRun = wfRun;

            wfEvent.record();

            ctx.json(response);

        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            ctx.status(500);
            response.status = ResponseStatus.INTERNAL_ERROR;
            response.message = exn.getMessage();
            ctx.json(response);
        }
    }

}
