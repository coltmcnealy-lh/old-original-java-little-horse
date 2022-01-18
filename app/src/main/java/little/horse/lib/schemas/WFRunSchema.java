package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import little.horse.lib.LHDatabaseClient;
import little.horse.lib.LHFailureReason;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHUtil;
import little.horse.lib.WFEventType;
import little.horse.lib.wfRuntime.WFRunStatus;

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
        passConfig(spec);
    }

    @JsonIgnore
    public WFSpecSchema getWFSpec() throws LHNoConfigException, LHLookupException {
        if (wfSpec == null) {
            if (config == null) {
                throw new LHNoConfigException(
                    "Tried to get WFSpec but no config provided. OrzDash!"
                );
            }
            String id = (wfSpecGuid == null) ? wfSpecName : wfSpecGuid;
            wfSpec = LHDatabaseClient.lookupWFSpec(id, config);
        }
        return wfSpec;
    }

    public ThreadRunMetaSchema addThread(
        String threadName,
        Map<String, Object> variables,
        WFRunStatus initialStatus,
        TaskRunSchema parentTaskRun
    ) throws LHNoConfigException, LHLookupException {
        ThreadRunSchema thread = addThread(threadName, variables, initialStatus);
        thread.parentThreadID = parentTaskRun.threadID;

        // lmao that's a lot of chaining haha
        parentTaskRun.parentThread.childThreadIDs.add(thread.id);

        if (awaitableThreads.get(parentTaskRun.nodeName) == null) {
            awaitableThreads.put(
                parentTaskRun.nodeName, new ArrayList<ThreadRunMetaSchema>()
            );
        }

        ThreadRunMetaSchema meta = new ThreadRunMetaSchema();
        passConfig(meta);
        meta.sourceNodeGuid = parentTaskRun.nodeGuid;
        meta.sourceNodeName = parentTaskRun.nodeName;
        meta.threadID = thread.id;
        meta.timesAwaited = 0;
        meta.parentThreadID = parentTaskRun.threadID;
        meta.threadSpecName = parentTaskRun.parentThread.threadSpecName;
        awaitableThreads.get(parentTaskRun.nodeName).add(meta);

        return meta;
    }

    public ThreadRunSchema addThread(
        String threadName, Map<String, Object> variables, WFRunStatus initialStatus
    ) throws LHNoConfigException, LHLookupException {
        getWFSpec();  // just make sure the thing isn't null;

        // Since the wfSpec has already been validated (at the time it was created)
        // via the API, this is supposedly hypotentially in theory guaranteed to
        // return a ThreadSpecSchema (otherwise there would've been an error thrown
        // at WFSpec creation time).
        ThreadSpecSchema tspec = wfSpec.threadSpecs.get(threadName);
        passConfig(tspec);

        ThreadRunSchema trun = new ThreadRunSchema();
        passConfig(trun);

        trun.id = threadRuns.size();
        trun.status = initialStatus;
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

        // Now add the entrypoint taskRun
        EdgeSchema fakeEdge = new EdgeSchema();
        fakeEdge.sinkNodeName = tspec.entrypointNodeName;
        trun.addEdgeToUpNext(fakeEdge);

        return trun;
    }

    @JsonIgnore
    public WFEventSchema newWFEvent(WFEventType type, BaseSchema content) {
        WFEventSchema event = new WFEventSchema();
        event.type = type;
        event.wfRunGuid = guid;
        event.wfSpecGuid = wfSpecGuid;
        event.wfSpecName = wfSpecName;
        event.timestamp = LHUtil.now();
        event.content = content.toString();
        event.wfRun = this;
        passConfig(event);
        return event;
    }

    private void recordExternalEvent(WFEventSchema event) {
        throw new RuntimeException("implement me");
    }

    public void incorporateEvent(WFEventSchema event)
    throws LHNoConfigException, LHLookupException {
        if (event.type == WFEventType.WF_RUN_STARTED) {
            throw new RuntimeException(
                "This shouldn't happen, colty you're programming like you're drunk"
            );
        }

        if (event.type == WFEventType.EXTERNAL_EVENT) {
            recordExternalEvent(event);
            return;
        }

        if (event.type == WFEventType.TASK_EVENT) {
            ThreadRunSchema thread = threadRuns.get(event.threadID);
            thread.incorporateEvent(event);
        }
    }

    public void updateStatuses() {
        for (ThreadRunSchema thread: threadRuns) {
            thread.updateStatus();
        }

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
            boolean allCompleted = true;
            for (ThreadRunSchema thread: this.threadRuns) {
                if (thread.status == WFRunStatus.RUNNING) {
                    allCompleted = false;
                } else if (thread.status != WFRunStatus.COMPLETED) {
                    LHUtil.log("WTF is this? Got a halted or halting thread but wfrun is running");
                }
            }
            if (allCompleted) {
                this.status = WFRunStatus.COMPLETED;
            }
        }
    }
}
