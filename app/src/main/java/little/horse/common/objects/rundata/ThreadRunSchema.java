package little.horse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import little.horse.common.Config;
import little.horse.common.events.ExternalEventCorrelSchema;
import little.horse.common.events.ExternalEventPayloadSchema;
import little.horse.common.events.TaskRunEndedEventSchema;
import little.horse.common.events.TaskRunEventSchema;
import little.horse.common.events.TaskRunStartedEventSchema;
import little.horse.common.events.TaskScheduledEventSchema;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.VarSubOrzDash;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.EdgeConditionSchema;
import little.horse.common.objects.metadata.EdgeSchema;
import little.horse.common.objects.metadata.ExceptionHandlerSpecSchema;
import little.horse.common.objects.metadata.InterruptDefSchema;
import little.horse.common.objects.metadata.NodeSchema;
import little.horse.common.objects.metadata.NodeType;
import little.horse.common.objects.metadata.ThreadSpecSchema;
import little.horse.common.objects.metadata.VariableAssignmentSchema;
import little.horse.common.objects.metadata.VariableMutationOperation;
import little.horse.common.objects.metadata.VariableMutationSchema;
import little.horse.common.objects.metadata.WFRunVariableDefSchema;
import little.horse.common.objects.metadata.WFRunVariableTypeEnum;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.util.LHUtil;

public class ThreadRunSchema extends BaseSchema {
    public String threadSpecName;
    public String threadSpecGuid;

    @JsonManagedReference
    public ArrayList<TaskRunSchema> taskRuns;
    public ArrayList<EdgeSchema> upNext;
    public WFRunStatus status;

    public HashMap<String, Object> variables;

    public int id;
    public Integer parentThreadID;
    public ArrayList<Integer> childThreadIDs;
    public ArrayList<Integer> activeInterruptThreadIDs;
    public ArrayList<Integer> handledInterruptThreadIDs;
    public Integer exceptionHandlerThread = null;
    public ArrayList<Integer> completedExeptionHandlerThreads;

    public String errorMessage;

    // should be set when this thread is actually an interrupt. Then we know the
    // parentThreadID field above refers to a thread that was interrupted by this
    // thread.
    public boolean isInterruptThread = false;

    public String exceptionName;

    // Map from variable name to threadID of thread holding lock on the variable.
    public HashMap<String, Integer> variableLocks;

    public HashSet<WFHaltReasonEnum> haltReasons;

    @JsonIgnore
    private ThreadSpecSchema privateThreadSpec;

    @JsonBackReference
    public WFRunSchema wfRun;

    @JsonIgnore
    public ThreadSpecSchema threadSpec;

    @JsonIgnore
    private ThreadSpecSchema getThreadSpec()
    throws LHConnectionError {
        if (threadSpec != null) return threadSpec;
        if (wfRun == null) {
            throw new RuntimeException(
                "parent wfRun isn't set and config isn't set!!"
            );
        }
        threadSpec = wfRun.getWFSpec().threadSpecs.get(threadSpecName);
        return threadSpec;
    }

    /**
     * Returns all variables currently visible to this ThreadRun. It loads the
     * variables local to this ThreadRun, then recursively merges that with all
     * variables visible to the parent (if it has a parent thread).
     * @return a HashMap of <String, Object> mapping variable name to value for
     * every variable that's visible to this thread.
     */
    @JsonIgnore
    public HashMap<String, Object> getAllVariables() {
        if (wfRun == null) {
            throw new RuntimeException(
                "the wfRUn wasn't set for this threadRun before calling getvars!"
            );
        }
        @SuppressWarnings("unchecked")
        HashMap<String, Object> out = (HashMap<String, Object>) variables.clone();

        // Yay, recursion!
        if (parentThreadID != null) {
            ThreadRunSchema parent = wfRun.threadRuns.get(parentThreadID);
            out.putAll(parent.getAllVariables());
        }

        return out;
    }

    /**
     * Finds the variable definition for a given variable name, and returns a pair
     * containing the variable definition and the ThreadRun from which that variable
     * was defined.
     * @param varName the variable name to find.
     * @return a tuple of WFRunVariableDefSchema, ThreadRunSchema: The definition
     * of the variable, and the parent ThreadRun that owns it. Returns null
     * if the variable is not in scope of this thread or doesn't exist.
     */
    @JsonIgnore
    public VariableLookupResult getVariableDefinition(
        String varName
    ) throws LHConnectionError {
        if (wfRun == null) {
            throw new RuntimeException("wfRun was not set yet!");
        }
        WFSpecSchema wfSpec = wfRun.getWFSpec();
        ThreadSpecSchema threadSchema = wfSpec.threadSpecs.get(threadSpecName);

        WFRunVariableDefSchema varDef = threadSchema.variableDefs.get(varName);
        if (varDef != null) {
            return new VariableLookupResult(varDef, this, variables.get(varName));
        }

        if (parentThreadID != null) {
            ThreadRunSchema parent = wfRun.threadRuns.get(parentThreadID);
            return parent.getVariableDefinition(varName);
        }

        // out of luck
        return null;
    }

    @JsonIgnore
    public Object getMutationRHS(
        VariableMutationSchema mutSchema, TaskRunSchema tr
    ) throws LHConnectionError, VarSubOrzDash {
        if (mutSchema.copyDirectlyFromNodeOutput) {
            return tr.stdout;
        } else if (mutSchema.jsonPath != null) {
            return LHUtil.jsonPath(tr.toString(), mutSchema.jsonPath);
        } else if (mutSchema.sourceVariable != null) {
            return assignVariable(mutSchema.sourceVariable);
        } else {
            return mutSchema.literalValue;
        }
    }

    @JsonIgnore
    public Object assignVariable(VariableAssignmentSchema var)
    throws LHConnectionError, VarSubOrzDash {
        if (var.literalValue != null) {
            return var.literalValue;
        }

        Object dataToParse = null;
        if (var.wfRunVariableName != null) {
            VariableLookupResult varLookup = getVariableDefinition(
                var.wfRunVariableName
            );
            Object result = varLookup.value;
            if (result == null) {
                throw new VarSubOrzDash(
                    null,
                    "No variable named " + var.wfRunVariableName + " in context."
                );
            }
            dataToParse = result;
        } else if (var.wfRunMetadata != null) {
            if (var.wfRunMetadata == WFRunMetadataEnum.WF_RUN_GUID) {
                return wfRun.guid;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.WF_SPEC_GUID) {
                return wfRun.wfSpecGuid;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.WF_SPEC_NAME) {
                return wfRun.wfSpecName;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.THREAD_GUID) {
                return String.valueOf(this.id) + "-"+ wfRun.guid;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.THREAD_ID) {
                return Integer.valueOf(this.id);
            }
        }

        if (dataToParse == null) {
            // Then we need to have a literal value.
            assert (var.defaultValue != null);
            return var.defaultValue;
        }
        if (var.jsonPath == null) {
            // just return the whole thing
            return dataToParse;
        }

        try {
            return LHUtil.jsonPath(dataToParse.toString(), var.jsonPath);
        } catch(Exception exn) {
            throw new VarSubOrzDash(
                exn,
                "Specified jsonpath " + var.jsonPath + " failed to resolve on " + dataToParse
            );
        }
    }

    @JsonIgnore
    public void addEdgeToUpNext(EdgeSchema edge) {
        upNext.add(edge);
    }

    @JsonIgnore
    public TaskRunSchema createNewTaskRun(NodeSchema node) 
    throws LHConnectionError {
        TaskRunSchema tr = new TaskRunSchema();
        tr.status = LHStatus.PENDING;
        tr.threadID = id;
        tr.number = taskRuns.size();
        tr.nodeName = node.name;
        // tr.nodeGuid = node.guid;
        tr.wfSpecGuid = wfRun.getWFSpec().getDigest();
        tr.wfSpecName = wfRun.getWFSpec().name;

        tr.parentThread = this;

        throw new RuntimeException("Oops");
        // return tr;
    }

    @JsonIgnore
    public void incorporateEvent(WFEventSchema wfEvent)
    throws LHConnectionError {
        TaskRunEventSchema event = BaseSchema.fromString(
            wfEvent.content, TaskRunEventSchema.class, config, false
        );
        if (event.startedEvent != null) {
            handleTaskStarted(event);
        } else if (event.endedEvent != null) {
            handleTaskEnded(event);
        }
    }

    // Potentially this and handleTaskEnded could go in the TaskRunSchema.java,
    // but I'm not sure I wanna deal with jumping back and forth like that.
    @JsonIgnore
    private void handleTaskStarted(TaskRunEventSchema trEvent) {
        TaskRunSchema tr = taskRuns.get(trEvent.taskRunNumber);
        TaskRunStartedEventSchema event = trEvent.startedEvent;

        tr.status = LHStatus.RUNNING;
        tr.startTime = trEvent.timestamp;
        tr.bashCommand = event.bashCommand;
        tr.stdin = event.stdin;
    }

    @JsonIgnore
    public WFEventSchema newWFEvent(WFEventType type, BaseSchema content) {
        WFEventSchema out = wfRun.newWFEvent(type, content);
        out.threadID = id;
        return out;
    }

    @JsonIgnore
    private void completeTask(
        TaskRunSchema task,
        LHStatus taskStatus,
        String stdout,
        String stderr,
        Date endTime,
        int returnCode
    ) throws LHConnectionError {
        task.endTime = endTime;
        task.stdout = LHUtil.jsonifyIfPossible(stdout);
        task.stderr = LHUtil.jsonifyIfPossible(stderr);
        task.status = taskStatus;
        task.returnCode = returnCode;

        unlockVariables(task.getNode());

        // Need the up next to be set whether or not the task fails/there is
        // a retry/it succeeds.
        upNext = new ArrayList<EdgeSchema>();
        for (EdgeSchema edge: task.getNode().outgoingEdges) {
            upNext.add(edge);
        }

        if (taskStatus == LHStatus.COMPLETED) {
            try {
                mutateVariables(task);
            } catch(VarSubOrzDash exn) {
                failTask(
                    task, LHFailureReason.VARIABLE_LOOKUP_ERROR,exn.getMessage()
                );
                return;
            }

        } else {
            failTask(
                task, LHFailureReason.TASK_FAILURE,
                "thread failed on node " + task.nodeName + ": " + stderr
            );
        }
    }

    @JsonIgnore
    private void handleTaskEnded(TaskRunEventSchema trEvent)
    throws LHConnectionError {
        TaskRunSchema tr = taskRuns.get(trEvent.taskRunNumber);
        TaskRunEndedEventSchema event = trEvent.endedEvent;
        LHStatus taskStatus = event.success ? LHStatus.COMPLETED : LHStatus.ERROR;

        completeTask(
            tr, taskStatus, event.stdout, event.stderr, trEvent.timestamp,
            event.returncode
        );
    }

    @JsonIgnore
    public void mutateVariables(TaskRunSchema tr) 
    throws VarSubOrzDash, LHConnectionError {

        // We need to do this atomically—-i.e. if there's one variable substitution
        // failure, none of the variables get mutated at all. Therefore we compute
        // all of the new values first and then assign them later once we're sure
        // there are no VarSubOrzDash's.
        ArrayList<Mutation> mutations = new ArrayList<Mutation>();

        for (Map.Entry<String, VariableMutationSchema> pair:
            tr.getNode().variableMutations.entrySet())
        {
            String varName = pair.getKey();
            VariableMutationSchema mutSchema = pair.getValue();
            VariableLookupResult varLookup = getVariableDefinition(varName);

            WFRunVariableDefSchema varDef = varLookup.varDef;
            ThreadRunSchema thread = varLookup.thread;
            Object lhs = varLookup.value;
            Object rhs = getMutationRHS(mutSchema, tr);
            VariableMutationOperation op = mutSchema.operation;

            // Ok, if we got this far, then we know that the RHS and LHS both exist,
            // but are LHS + operation + RHS valid?
            Mutation mut = new Mutation(lhs, rhs, op, thread, varDef, varName);
            mut.execute(true);  // validate by call with dryRun==true
            mutations.add(mut);
        }

        // If we've gotten this far, then we know (if I coded everything properly)
        // that we aren't going to have an error when we finally apply the
        // mutations.
        for (Mutation mutation: mutations) {
            mutation.execute(false);
        }
    }

    @JsonIgnore
    private void handleException(
        String handlerSpecName, TaskRunSchema tr, LHFailureReason reason, String msg
    ) throws LHConnectionError {
        tr.status = LHStatus.ERROR;
        tr.failureMessage = msg;
        tr.failureReason = reason;

        ThreadRunSchema handler = wfRun.createThreadClientAdds(
            handlerSpecName,
            // In the future we may pass info to the handler thread.
            new HashMap<String, Object>(),
            this
        );
        wfRun.threadRuns.add(handler);
        exceptionHandlerThread = handler.id;

        // Important to do this after creating the exception handler thread
        // so that we don't propagate HALTED/ING status to it.
        halt(
            WFHaltReasonEnum.HANDLING_EXCEPTION,
            "Handling exception on failed task " + tr.nodeName
        );
    }

    @JsonIgnore
    private void failTask(
        TaskRunSchema tr, LHFailureReason reason, String message
    ) throws LHConnectionError {
        tr.status = LHStatus.ERROR;
        tr.failureMessage = message;
        tr.failureReason = reason;

        // TODO: Determine whether or not to enqueue a retry.

        if (tr.getNode().baseExceptionhandler != null) {
            // Treat the exception handler LIKE an interrupt, but not really.
            String tname = tr.getNode().baseExceptionhandler.handlerThreadSpecName;
            handleException(tname, tr, reason, message);
        } else {
            halt(
                WFHaltReasonEnum.FAILED,
                "Thread " + String.valueOf(id) + " failed on task "
                + tr.nodeName + ": " + message
            );
        }
    }

    @JsonIgnore
    boolean evaluateEdge(EdgeConditionSchema condition)
    throws VarSubOrzDash, LHConnectionError {
        if (condition == null) return true;
        Object lhs = assignVariable(condition.leftSide);
        Object rhs = assignVariable(condition.rightSide);
        switch (condition.comparator) {
            case LESS_THAN: return Mutation.compare(lhs, rhs) < 0;
            case LESS_THAN_EQ: return Mutation.compare(lhs, rhs) <= 0;
            case GREATER_THAN: return Mutation.compare(lhs, rhs) > 0;
            case GRREATER_THAN_EQ: return Mutation.compare(lhs, rhs) >= 0;
            case EQUALS: return lhs != null && lhs.equals(rhs);
            case NOT_EQUALS: return lhs != null && !lhs.equals(rhs);
            case IN: return Mutation.contains(lhs, rhs);
            case NOT_IN: return !Mutation.contains(lhs, rhs);
            default: return false;
        }
    }

    private void log(Object... things) {
        if (id != 0) return;

        LHUtil.logBack(1, things);
    }

    public void updateStatus() {
        if (isCompleted()) return;
        if (upNext == null) upNext = new ArrayList<EdgeSchema>();

        if (status == WFRunStatus.RUNNING) {
            // If there are no pending taskruns and the last one executed was
            // COMPLETED, then the thread is now completed.
            if (upNext == null || upNext.size() == 0) {
                TaskRunSchema lastTr = taskRuns.size() > 0 ?
                    taskRuns.get(taskRuns.size() - 1) : null;
                if (lastTr == null || lastTr.isCompleted()) {
                    status = WFRunStatus.COMPLETED;
                }
            } else {
                if (taskRuns.size() > 0) {
                    TaskRunSchema lastTr = taskRuns.get(taskRuns.size() - 1);
                    if (lastTr.status == LHStatus.ERROR) {
                        status = WFRunStatus.HALTED;
                    }
                }
            }

        } else if (status == WFRunStatus.HALTED) {
            // Check if interrupt handlers are done now (:
            for (int i = activeInterruptThreadIDs.size() - 1; i >= 0; i--) {
                int tid = activeInterruptThreadIDs.get(i);
                if (tid >= wfRun.threadRuns.size()) continue;

                ThreadRunSchema intHandler = wfRun.threadRuns.get(tid);
                if (intHandler.isCompleted()) {
                    activeInterruptThreadIDs.remove(i);
                    handledInterruptThreadIDs.add(intHandler.id);
                }
            }
            if (haltReasons.contains(WFHaltReasonEnum.INTERRUPT)
                && activeInterruptThreadIDs.size() == 0
            ) {
                removeHaltReason(WFHaltReasonEnum.INTERRUPT);
            }

            // Check if we just fixed an exception handler
            if (exceptionHandlerThread != null) {
                ThreadRunSchema handler = wfRun.threadRuns.get(
                    exceptionHandlerThread
                );

                if (handler.isCompleted()) {
                    removeHaltReason(WFHaltReasonEnum.HANDLING_EXCEPTION);
                } else if (handler.isFailed()) {
                    // we're failed.
                    halt(WFHaltReasonEnum.FAILED,
                        "Exception handler on thread " + handler.id + " failed!"
                    );
                } else {
                    LHUtil.log("waiting for exception handler to finish");
                }
            }
        } else if (status == WFRunStatus.HALTING) {
            // Well we just gotta see if the last task run is done.
            if (taskRuns.size() == 0 || taskRuns.get(
                    taskRuns.size() - 1
            ).isTerminated()) {
                status = WFRunStatus.HALTED;
            }
        }
    }

    public boolean isLocked(String variableName, int threadID) {
        if (variables.containsKey(variableName)) {
            Integer lockingThread = variableLocks.get(variableName);
            return lockingThread != null && lockingThread != threadID;
        }
        if (parentThreadID != null) {
            return wfRun.threadRuns.get(
                parentThreadID
            ).isLocked(variableName, threadID);
        }
        throw new RuntimeException("Impossible to get here since it means");
    }

    public void lock(String variableName, int threadID) {
        if (variables.containsKey(variableName)) {
            variableLocks.put(variableName, threadID);
        } else if (parentThreadID != null) {
            wfRun.threadRuns.get(parentThreadID).lock(variableName, threadID);
        } else {
            throw new RuntimeException("Impossible");
        }
    }

    public void unlock(String variableName) {
        if (variables.containsKey(variableName)) {
            variableLocks.remove(variableName);
        } else if (parentThreadID != null) {
            wfRun.threadRuns.get(parentThreadID).unlock(variableName);
        }
    }

    @JsonIgnore
    public boolean lockVariables(NodeSchema n, int threadID) {
        HashSet<String> neededVars = WFRunSchema.getNeededVars(n);

        // Now check to make sure that no one is using the variables we need.
        for (String var: neededVars) {
            if (isLocked(var, threadID)) return false;
        }

        // if we got this far, then we are all clear. Lock every variable and go
        // from there.
        for (String var: neededVars) {
            lock(var, threadID);
        }
        return true;
    }

    @JsonIgnore
    public void unlockVariables(NodeSchema n) {
        for (String var: WFRunSchema.getNeededVars(n)) {
            unlock(var);
        }
    }

    @JsonIgnore
    public boolean advance(WFEventSchema event)
    throws LHConnectionError {
        if (status != WFRunStatus.RUNNING || upNext.size() == 0) {
            return false;
        }

        // If we get here, we know we have a running thread, and there's an Edge in
        // the upNext that is waiting to be fired.
        if (upNextEdgesBlocked()) {
            return false;
        }

        // Now we have the green light to determine whether any of the edges will
        // fire.
        boolean shouldClear = true;
        NodeSchema activatedNode = null;
        for (EdgeSchema edge: upNext) {
            try {
                if (evaluateEdge(edge.condition)) {
                    NodeSchema n = getThreadSpec().nodes.get(edge.sinkNodeName);
                    if (lockVariables(n, id)) {
                        activatedNode = n;
                        break;
                    }
                    // If we get here, we know there's still stuff to do, but we
                    // can't do it yet because we're blocked. This means don't clear
                    // the upNext taskRuns.
                    shouldClear = false;
                }
                // If we got here without returning, then we know that there are no
                // taskRuns left.
            } catch(VarSubOrzDash exn) {
                TaskRunSchema lastTr = taskRuns.get(taskRuns.size() - 1);
                exn.printStackTrace();
                failTask(
                    lastTr,
                    LHFailureReason.VARIABLE_LOOKUP_ERROR,
                    "Failed substituting variable when processing if condition: " +
                    exn.getMessage()
                );
                return true;
            }
        }

        if (activatedNode == null && shouldClear) {
            upNext = new ArrayList<EdgeSchema>();
            return true;
        }

        if (activatedNode == null && !shouldClear) {
            // then we're blocked but nothing changed.
            return false;
        }

        return activateNode(activatedNode, event);
    }

    @JsonIgnore
    private void scheduleTask(TaskRunSchema tr, NodeSchema node) {
        TaskScheduledEventSchema te = new TaskScheduledEventSchema();
        te.setConfig(config);
        te.taskType = node.taskDef.taskType;
        te.taskQueueName = node.taskDef.taskQueueName;
        te.wfRunGuid = wfRun.guid;
        te.wfSpecGuid = wfRun.wfSpecGuid;
        te.wfSpecName = wfRun.wfSpecName;
        te.taskExecutionGuid = wfRun.guid + "_" + String.valueOf(id) + "_" +
            String.valueOf(taskRuns.size());

        te.record();
    }

    @JsonIgnore
    private boolean activateNode(
        NodeSchema node, WFEventSchema event)
    throws LHConnectionError {
        if (node.nodeType == NodeType.TASK) {
            upNext = new ArrayList<EdgeSchema>();
            TaskRunSchema tr = createNewTaskRun(node);
            taskRuns.add(tr);
            log("Adding node on ", tr.nodeName, "length is ", taskRuns.size());
            LHUtil.log("actor.act", id, tr.nodeName);

            scheduleTask(tr, node);
            return true;

        } else if (node.nodeType == NodeType.EXTERNAL_EVENT) {
            ArrayList<ExternalEventCorrelSchema> relevantEvents =
            wfRun.correlatedEvents.get(node.externalEventDefName);
            if (relevantEvents == null) {
                relevantEvents = new ArrayList<ExternalEventCorrelSchema>();
                wfRun.correlatedEvents.put(node.externalEventDefName, relevantEvents);
            }
            ExternalEventCorrelSchema correlSchema = null;
            
            for (ExternalEventCorrelSchema candidate : relevantEvents) {
                // In the future, we may want to add the ability to signal
                // a specific thread rather than the whole wfRun. We would do that here.
                if (candidate.event != null && candidate.assignedNodeGuid == null) {
                    correlSchema = candidate;
                }
            }
            if (correlSchema == null) return false;  // Still waiting nothing changed

            TaskRunSchema tr = createNewTaskRun(node);
            taskRuns.add(tr);
            throw new RuntimeException("oops");
            // correlSchema.assignedNodeGuid = node.guid;
            // correlSchema.assignedNodeName = node.name;
            // correlSchema.assignedTaskRunExecutionNumber = tr.number;
            // correlSchema.assignedThreadID = tr.threadID;

            // completeTask(
            //     tr, LHStatus.COMPLETED, correlSchema.event.content.toString(),
            //     null, correlSchema.event.timestamp, 0
            // );
            // upNext = new ArrayList<EdgeSchema>();
            // return true; // Obviously something changed, we done did add a task.

        } else if (node.nodeType == NodeType.SPAWN_THREAD) {
            upNext = new ArrayList<EdgeSchema>();
            HashMap<String, Object> inputVars = new HashMap<String, Object>();
            TaskRunSchema tr = createNewTaskRun(node);
            try {
                for (Map.Entry<String, VariableAssignmentSchema> pair:
                    node.variables.entrySet()
                ) {
                    inputVars.put(pair.getKey(), assignVariable(pair.getValue()));
                }

            } catch(VarSubOrzDash exn) {
                exn.printStackTrace();
                failTask(
                    tr, LHFailureReason.VARIABLE_LOOKUP_ERROR,
                    "Failed creating variables for subthread: " + exn.getMessage()
                );
                return true;
            }

            ThreadRunSchema thread = wfRun.createThreadClientAdds(
                node.threadSpawnThreadSpecName, inputVars, this
            );
            wfRun.threadRuns.add(thread);
    
            if (wfRun.awaitableThreads.get(tr.nodeName) == null) {
                wfRun.awaitableThreads.put(
                    tr.nodeName, new ArrayList<ThreadRunMetaSchema>()
                );
            }

            ThreadRunMetaSchema meta = new ThreadRunMetaSchema(tr, thread);
            wfRun.awaitableThreads.get(tr.nodeName).add(meta);
            taskRuns.add(tr);
            completeTask(
                tr, LHStatus.COMPLETED, meta.toString(), null, event.timestamp, 0
            );
            return true;

        } else if (node.nodeType == NodeType.WAIT_FOR_THREAD) {
            return handleWaitForThreadNode(node, event);
        } else if (node.nodeType == NodeType.THROW_EXCEPTION) {
            TaskRunSchema tr = createNewTaskRun(node);
            taskRuns.add(tr);
            exceptionName = node.exceptionToThrow;
            completeTask(
                tr, LHStatus.ERROR, "", "Throwing exception " + exceptionName,
                event.timestamp, -1
            );
            return true;
        }
        throw new RuntimeException("invalid node type: " + node.nodeType);
    }

    private boolean handleWaitForThreadNode(
        NodeSchema node, WFEventSchema event
    ) throws LHConnectionError {
        // Iterate through all of the ThreadRunMetaSchema's in the wfRun.
        // If it's from the node we're waiting for and it's NOT done, then
        // this node does nothing. But if it's already done, we mark it as
        // awaited and continue on. If all of the relevant threads are done,
        // then this node completes.
        TaskRunSchema tr = createNewTaskRun(node);
        ArrayList<ThreadRunMetaSchema> awaitables = wfRun.awaitableThreads.get(
            node.threadWaitSourceNodeName
        );

        if (awaitables == null) {
            taskRuns.add(tr);
            failTask(
                tr, LHFailureReason.INVALID_WF_SPEC_ERROR,
                "Got to node " + node.name + " which waits for a thread from " +
                node.threadWaitSourceNodeName + " but no threads have started" +
                " from the specified source node."
            );
            return true;
        }
        boolean allTerminated = true;
        boolean allCompleted = true;

        for (ThreadRunMetaSchema meta: awaitables) {
            ThreadRunSchema thread = wfRun.threadRuns.get(meta.threadID);
            if (!thread.isCompleted()) {
                allCompleted = false;
            }
            if (!thread.isTerminated()) {
                allTerminated = false;
            }
        }

        // Still waiting for some children to do their thing.
        if (!allTerminated) {
            return false;
        }

        // If we got here, we know all the threads have finished. Let's handle the
        // simplest case first--the threads succeeded:
        if (allCompleted) {
            taskRuns.add(tr);
            completeTask(
                tr, LHStatus.COMPLETED, awaitables.toString(),
                null, event.timestamp, 0
            );
        } else {
            // TODO: We're going to combine the exception handler infra with the
            // interrupt handler infra. After we do that, we're going to be able to
            // handle more than one exception at once, which will enable us to join
            // multiple threads at once.
            if (awaitables.size() != 1) {
                throw new RuntimeException(
                    "TODO: handle joins of more than one thread at once."
                );
            }

            ThreadRunSchema thread = wfRun.threadRuns.get(awaitables.get(0).threadID);
            if (thread.isCompleted() || !thread.isTerminated()) {
                throw new RuntimeException("should be impossible");
            }

            ExceptionHandlerSpecSchema hspec = node.getHandlerSpec(
                thread.exceptionName
            );
            if (hspec == null) {

            } else {
                String msg = "TaskRun on " + tr.nodeName +
                " Failed with exception " + hspec.handlerThreadSpecName + ", so" +
                " we are handling it.";

                completeTask(
                    tr, LHStatus.ERROR, awaitables.toString(), msg, event.timestamp, 1
                );
                handleException(
                    hspec.handlerThreadSpecName, tr, LHFailureReason.TASK_FAILURE, msg
                );
            }
        }

        return true;
    }

    /**
     * Determine whether the edges in `this.upNext` are blocked because a TaskRun in
     * another ThreadRun is using a certain variable that the edges need.
     * @return true if the edges are Blocked, false if they're UNBlocked.
     */
    @JsonIgnore
    private boolean upNextEdgesBlocked() {
        // Will implement this once we got some threads going.
        return false;
    }

    @JsonIgnore
    public ArrayList<ThreadRunSchema> getChildren() {
        ArrayList<ThreadRunSchema> out = new ArrayList<ThreadRunSchema>();
        for (int tid: childThreadIDs) {
            out.add(wfRun.threadRuns.get(tid));
        }
        return out;
    }

    /**
     * Halts this WFRun and its children.
     * @param event WFEventSchema triggering the halt.
     */
    @JsonIgnore
    public void halt(WFHaltReasonEnum reason, String message) {
        if (status == WFRunStatus.RUNNING) {
            status = WFRunStatus.HALTING;
            errorMessage += message + "\n";
        } else if (isCompleted()) {
            LHUtil.log(
                "Somehow we find ourself in the halt() method on a completed",
                "thread, which might mean that this is a child thread who has",
                "already returned back to his dad."
            );
        }
        haltReasons.add(reason);

        for (ThreadRunSchema kid: getChildren()) {
            if (kid.isInterruptThread && reason == WFHaltReasonEnum.INTERRUPT) {
                continue;
            }
            if (exceptionHandlerThread != null && kid.id == exceptionHandlerThread &&
                reason == WFHaltReasonEnum.HANDLING_EXCEPTION
            ) {
                continue;
            }
            kid.halt(WFHaltReasonEnum.PARENT_STOPPED, "Parent thread was halted.");
        }
    }

    @JsonIgnore
    public void removeHaltReason(WFHaltReasonEnum reason) {
        haltReasons.remove(reason);

        if (haltReasons.isEmpty()) {
            if (status == WFRunStatus.HALTED || status == WFRunStatus.HALTING) {
                status = WFRunStatus.RUNNING;
                errorMessage = "";
            }

            for (ThreadRunSchema kid: getChildren()) {
                kid.removeHaltReason(WFHaltReasonEnum.PARENT_STOPPED);
            }
        } else if (haltReasons.size() == 1
            && haltReasons.contains(WFHaltReasonEnum.INTERRUPT)
        ) {
            // In this case, the only thing holding up the parent is one (or more)
            // interrupt threads. Those threads shouldn't be blocked by the parent
            // at this point, so we unblock them.
            for (ThreadRunSchema kid: getChildren()) {
                // Only unblock the interrupts!!!
                if (kid.isInterruptThread) {
                    kid.removeHaltReason(WFHaltReasonEnum.PARENT_INTERRUPTED);
                }
            }
        }
    }

    @JsonIgnore
    public void handleInterrupt(ExternalEventPayloadSchema payload) throws
    LHConnectionError {

        HashMap<String, InterruptDefSchema> idefs = getThreadSpec().interruptDefs;
        InterruptDefSchema idef = idefs.get(payload.externalEventDefName);
        String tspecname = idef.threadSpecName;

        // crucial to create the thread BEFORE calling halt(), as the call to halt()
        // adds a WFHaltReason which we dont wanna propagate to the interrupt thread.
        ThreadRunSchema trun = wfRun.createThreadClientAdds(
            tspecname,
            LHUtil.unsplat(payload.content),
            this
        );
        trun.isInterruptThread = true;
        wfRun.threadRuns.add(trun);

        activeInterruptThreadIDs.add(trun.id);
        // Now we call halt.
        halt(WFHaltReasonEnum.INTERRUPT, "Halted for interrupt");
    }

    @JsonIgnore
    public boolean isFailed() {
        return (status == WFRunStatus.HALTED && haltReasons.contains(
            WFHaltReasonEnum.FAILED
        ));
    }

    @JsonIgnore
    public boolean isCompleted() {
        return (status == WFRunStatus.COMPLETED);
    }

    @JsonIgnore
    public boolean isTerminated() {
        return (isCompleted() || haltReasons.contains(WFHaltReasonEnum.FAILED));
    }

    @JsonIgnore
    public void propagateInterrupt(ExternalEventPayloadSchema payload) throws
    LHConnectionError {
        HashMap<String, InterruptDefSchema> idefs = getThreadSpec().interruptDefs;
        if (idefs != null && idefs.containsKey(payload.externalEventDefName)) {
            // Now we need to add thread!
            handleInterrupt(payload);
        } else {
            for (ThreadRunSchema kid: getChildren()) {
                kid.propagateInterrupt(payload);
            }
        }
    }

    @JsonIgnore
    @Override
    public Config setConfig(Config config) {
        super.setConfig(config);

        if (taskRuns == null) taskRuns = new ArrayList<>();
        for (TaskRunSchema taskRun: taskRuns) {
            taskRun.setConfig(config);
            taskRun.parentThread = this;
        }
    
        return this.config;
    }
}


class Mutation {
    public Object lhs;
    public Object rhs;
    public VariableMutationOperation op;
    public ThreadRunSchema tr;
    public WFRunVariableDefSchema varDef;
    public String varName;

    public Mutation(
        Object lhs, Object rhs, VariableMutationOperation op, ThreadRunSchema tr,
        WFRunVariableDefSchema varDef, String varName
    ) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.op = op;
        this.tr = tr;
        this.varDef = varDef;
        this.varName = varName;
    }

    public void execute(boolean dryRun) throws VarSubOrzDash {
        // Can't rely upon the WFRunVariableDefSchema because if, for example, the
        // LHS variable is an OBJECT, and there is a jsonpath, we could index from
        // the object to an Integer, in which case the LHS is not of the same type
        // as the varDef.type; but we will get more fancy once we add jsonschema
        // validation.
        Class<?> defTypeCls = lhs == null ? Object.class : lhs.getClass();

        // Now we handle every operation that's legal. Because I'm lazy, there's
        // only two so far.
        if (op == VariableMutationOperation.SET) {
            if (!defTypeCls.isInstance(rhs)) {
                throw new VarSubOrzDash(null,
                    "Tried to set var " + varName + ", which is of type " +
                    defTypeCls.getName() + " to " + rhs.toString() + ", which is " +
                    " of type " + rhs.getClass().getName()
                );
            }
            if (!dryRun) {
                tr.variables.put(varName, rhs);
            }

        } else if (op == VariableMutationOperation.ADD) {
            if (varDef.type == WFRunVariableTypeEnum.BOOLEAN ||
                varDef.type == WFRunVariableTypeEnum.OBJECT
            ) {
                throw new VarSubOrzDash(
                    null,
                    "had an invalid wfspec. Tried to add a boolean or object."
                );
            }

            try {
                // Just try to cast the right hand side to what it's supposed to be
                // in order to verify that it'll work.
                if (varDef.type == WFRunVariableTypeEnum.INT) {
                    Integer result = (Integer) rhs + (Integer) lhs;
                    if (!dryRun) {
                        tr.variables.put(varName, result);
                    }
                } else if (varDef.type == WFRunVariableTypeEnum.STRING) {
                    String result = (String) lhs + (String) rhs;
                    if (!dryRun) {
                        tr.variables.put(varName, result);
                    }
                } else if (varDef.type == WFRunVariableTypeEnum.ARRAY) {
                    // nothing to verify here until we start enforcing json schemas
                    // within arrays
                    @SuppressWarnings("unchecked")
                    ArrayList<Object> lhsArr = (ArrayList<Object>) lhs;
                    if (!dryRun) {
                        lhsArr.add(rhs);
                    }
                } else if (varDef.type == WFRunVariableTypeEnum.DOUBLE) {
                    Double result = (Double) lhs + (Double) rhs;
                    if (!dryRun) {
                        tr.variables.put(varName, result);
                    }
                }
            } catch(Exception exn) {
                throw new VarSubOrzDash(exn,
                    "Failed casting the value " + rhs.toString() + " to a " +
                    defTypeCls.getName()
                );
            }
        }
    }

    @SuppressWarnings("all")
    public static boolean contains(Object left, Object right) throws VarSubOrzDash {
        try {
            Collection<Object> collection = (Collection<Object>) left;
            for (Object thing : collection) {
                if (thing.equals(right)) {
                    return true;
                }
            }
        } catch (Exception exn) {
            exn.printStackTrace();
            throw new VarSubOrzDash(
                exn,
                "Failed determing whether the left contains the right "
            );
        }
        return false;

    }

    @SuppressWarnings("all") // lol
    public static int compare(Object left, Object right) throws VarSubOrzDash {

        try {
            int result = ((Comparable) left).compareTo((Comparable) right);
            return result;
        } catch(Exception exn) {
            LHUtil.logError(exn.getMessage());
            throw new VarSubOrzDash(exn, "Failed comparing the provided values.");
        }
    }
}
