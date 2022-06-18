package io.littlehorse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.littlehorse.api.ResponseStatus;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.events.ExternalEventCorrel;
import io.littlehorse.common.events.ExternalEventPayload;
import io.littlehorse.common.events.WFEvent;
import io.littlehorse.common.events.WFEventType;
import io.littlehorse.common.events.WFRunRequest;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.Edge;
import io.littlehorse.common.objects.metadata.ExternalEventDef;
import io.littlehorse.common.objects.metadata.GETable;
import io.littlehorse.common.objects.metadata.ThreadSpec;
import io.littlehorse.common.objects.metadata.WFRunVariableDef;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.util.LHDatabaseClient;
import io.littlehorse.common.util.LHRpcResponse;
import io.littlehorse.common.util.LHUtil;

@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "objectId",
    scope = WFRun.class
)
public class WFRun extends GETable {
    public String wfSpecDigest;
    public String wfSpecName;

    @JsonManagedReference
    public ArrayList<ThreadRun> threadRuns;

    public LHExecutionStatus status;
    public Date startTime;
    public Date endTime;

    public LHFailureReason errorCode;
    public String errorMessage;

    public HashMap<String, ArrayList<ExternalEventCorrel>> correlatedEvents;
    public Stack<String> pendingInterrupts;

    @JsonIgnore
    private WFSpec wfSpec;

    @JsonIgnore
    public void setWFSpec(WFSpec spec) {
        wfSpec = spec;
        wfSpecDigest = wfSpec.getObjectId();
        wfSpecName = wfSpec.name;
    }

    @JsonIgnore
    public WFSpec getWFSpec() throws LHConnectionError {
        if (wfSpec == null) {
            String id = (wfSpecDigest == null) ? wfSpecName : wfSpecDigest;
            setWFSpec(LHDatabaseClient.getByNameOrId(id, config, WFSpec.class));
        }
        return wfSpec;
    }

    @JsonIgnore
    public ThreadRun createThreadClientAdds(
        String threadName, Map<String, Object> variables, ThreadRun parent
    ) throws LHConnectionError {
        ThreadSpec tspec = wfSpec.threadSpecs.get(threadName);

        ThreadRun trun = new ThreadRun();
        trun.setConfig(config);

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
        trun.upNext = new ArrayList<>();
        trun.threadSpec = wfSpec.threadSpecs.get(threadName);
        trun.threadSpecName = threadName;
        if (parent != null) {
            trun.parentThreadId = parent.id;
        }

        trun.errorMessage = "";

        trun.activeInterruptThreadIds = new ArrayList<Integer>();
        trun.handledInterruptThreadIds = new ArrayList<Integer>();

        // Now add the entrypoint taskRun
        Edge fakeEdge = new Edge();
        fakeEdge.sinkNodeName = tspec.entrypointNodeName;
        trun.addEdgeToUpNext(fakeEdge);

        trun.childThreadIds = new ArrayList<>();
        trun.wfRun = this;
        trun.variableLocks = new HashMap<String, Integer>();

        trun.haltReasons = new HashSet<>();

        if (parent != null) {
            parent.childThreadIds.add(trun.id);
            trun.parentThreadId = parent.id;

            if (parent.status == LHExecutionStatus.HALTED ||
                parent.status == LHExecutionStatus.HALTING
            ) {
                trun.haltReasons.add(WFHaltReasonEnum.PARENT_STOPPED);
            }
        }

        return trun;
    }

    @JsonIgnore
    public WFEvent newWFEvent(WFEventType type, BaseSchema content) {
        WFEvent event = new WFEvent();
        event.setConfig(config);
        event.type = type;
        event.wfRunId = objectId;
        event.wfSpecId = wfSpecDigest;
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
            if (event.threadId != -1) {
                threadRuns.get(event.threadId).handleInterrupt(payload);
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
            correl.assignedThreadId = event.threadId;

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
            ThreadRun thread = threadRuns.get(event.threadId);
            thread.incorporateEvent(event);
        }

        if (event.type == WFEventType.WF_RUN_STOP_REQUEST) {
            if (event.threadId == 0 && status == LHExecutionStatus.RUNNING) {
                status = LHExecutionStatus.HALTING;
            }
            int threadID = event.threadId >= 0 ? event.threadId : 0;
            if (threadID < threadRuns.size()) {
                threadRuns.get(event.threadId).halt(
                    WFHaltReasonEnum.MANUAL_STOP,
                    "Manual halt of this thread requested by system admin."
                );
            }
        }

        if (event.type == WFEventType.WF_RUN_RESUME_REQUEST) {
            if (event.threadId == 0 && status != LHExecutionStatus.COMPLETED) {
                status = LHExecutionStatus.RUNNING;
            }
            if (event.threadId < threadRuns.size()) {
                threadRuns.get(event.threadId).removeHaltReason(
                    WFHaltReasonEnum.MANUAL_STOP
                );
            }
        }

        if (event.type == WFEventType.TIMER_EVENT) {
            handleTimerEvent(event);
        }
    }

    @JsonIgnore
    private void handleTimerEvent(WFEvent event) throws LHConnectionError {
        WFRunTimer timer;
        try {
            timer = BaseSchema.fromString(
                event.content, WFRunTimer.class, config
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            throw new RuntimeException("this is impossible");
        }

        ThreadRun thread = threadRuns.get(timer.threadRunId);
        thread.handleTimer(timer);
    }

    @JsonIgnore
    public void updateStatuses(WFEvent event) {
        for (ThreadRun thread: threadRuns) {
            thread.updateStatus();
        }

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
                endTime = event.timestamp;
                this.status = LHExecutionStatus.COMPLETED;
            } else if (allTerminated) {
                this.status = LHExecutionStatus.HALTED;
            }
        }
    }

    @JsonIgnore
    public ThreadRun entrypointThreadRun() {
        return threadRuns.get(0);
    }

    public static WFRunApiStuff apiStuff;

    public static void overridePostAPIEndpoints(Javalin app, LHConfig config) {
        apiStuff = new WFRunApiStuff(config);

        app.post("/WFRun", apiStuff::postRun);
        app.post(
            "/externalEvent/{externalEventDefId}/{wfRunId}", apiStuff::postEvent
        );

        app.post("/WFRun/stop/{wfRunId}/{tid}", apiStuff::postStopThread);
        app.post("/WFRun/resume/{wfRunId}/{tid}", apiStuff::postResumeThread);
    }

    @Override
    public void addIndexKeyValPairs(Map<String, String> pairs) {
        for (ThreadRun tr: threadRuns) {
            for (String varName: tr.variables.keySet()) {
                Object val = tr.variables.get(varName);
                String varKey = "var_" + varName;

                if (val == null) {
                    pairs.put(varKey, null);

                } else if (val instanceof String) {
                    pairs.put(varKey, String.class.cast(val));

                } else if (val instanceof Float) {
                    pairs.put(varKey, LHUtil.floatToDbString(Float.class.cast(val)));

                } else if (val instanceof Integer) {
                    pairs.put(varKey, LHUtil.intToDbString(Integer.class.cast(val)));

                } else if (val instanceof Boolean) {
                    pairs.put(varKey, LHUtil.boolToDbString(Boolean.class.cast(val)));

                } else if (val instanceof Map) {
                    LHUtil.log("TODO: figure out how to index a map");

                } else if (val instanceof List) {
                    LHUtil.log("TODO: Figure out how to index a list");

                }
            }
        }

        pairs.put("WFSpecName", wfSpecName);
        pairs.put("WFSpecId", wfSpecDigest);
    }

}

class WFRunApiStuff {
    private LHConfig config;

    public WFRunApiStuff(LHConfig config) {
        this.config = config;
    }

    public void postRun(Context ctx) {
        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        WFRunRequest request;
        try {
            request = BaseSchema.fromBytes(
                ctx.bodyAsBytes(),
                WFRunRequest.class,
                config
            );
        } catch(LHSerdeError exn) {
            response.status = ResponseStatus.VALIDATION_ERROR;
            response.message = exn.getMessage();
            ctx.json(response);
            return;
        }
        if (request.variables == null) {
            request.variables = new HashMap<>();
        }

        WFEvent event = new WFEvent();
        event.setConfig(config);
        String guid = (request.wfRunId == null) ?
            LHUtil.generateGuid():request.wfRunId;

        event.wfRunId = guid;
        event.content = request.toString();
        event.type = WFEventType.WF_RUN_STARTED;
        event.timestamp = LHUtil.now();

        try {
            WFSpec spec = LHDatabaseClient.getByNameOrId(
                request.wfSpecId,
                config,
                WFSpec.class
            );
            if (spec == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "Couldn't find wfspec " + request.wfSpecId;

            } else {
                response.objectId = guid;
                event.wfSpecId = spec.getObjectId();
                event.wfSpecName = spec.name;
                event.wfSpec = spec;
                event.record();
                response.status = ResponseStatus.OK;

            }
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
        String wfRunGuid = ctx.pathParam("wfRunId");
        int tid = Integer.valueOf(ctx.pathParam("tid"));

        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        try {

            WFRun wfRun = LHDatabaseClient.getByNameOrId(
                wfRunGuid, config, WFRun.class
            );
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
            event.threadId = tid;
            event.wfSpecId = event.wfRun.wfSpecDigest;
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
        String wfRunGuid = ctx.pathParam("wfRunId");
        int tid = Integer.valueOf(ctx.pathParam("tid"));

        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        try {

            WFRun wfRun = LHDatabaseClient.getByNameOrId(wfRunGuid, config, WFRun.class);
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
            event.threadId = tid;
            event.wfSpecId = event.wfRun.wfSpecDigest;
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
        String externalEventDefID = ctx.pathParam("externalEventDefId");
        Object eventContent;
        try {
            eventContent = ctx.bodyAsClass(Object.class);
        } catch(Exception exn) {
            eventContent = ctx.body();
        }

        WFRun wfRun;
        ExternalEventDef evd;

        LHRpcResponse<WFRun> response = new LHRpcResponse<>();

        try {
            wfRun = LHDatabaseClient.getByNameOrId(wfRunId, config, WFRun.class);
            if (wfRun == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "Couldn't find wfRun with id " + wfRunId;
                ctx.json(response);
                return;
            }

            evd = LHDatabaseClient.getByNameOrId(
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
            payload.externalEventDefId = evd.getObjectId();
            payload.externalEventDefName = evd.name;
            payload.content = eventContent;

            WFEvent wfEvent = new WFEvent();
            wfEvent.wfRunId = wfRun.getObjectId();
            wfEvent.wfSpecId = wfRun.wfSpecDigest;
            wfEvent.wfSpecName = wfRun.wfSpecName;
            wfEvent.type = WFEventType.EXTERNAL_EVENT;
            wfEvent.timestamp = LHUtil.now();
            wfEvent.content = payload.toString();
            wfEvent.wfRun = wfRun;
            wfEvent.setConfig(config);

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
