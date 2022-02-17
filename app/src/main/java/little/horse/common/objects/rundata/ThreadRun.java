package little.horse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.common.events.ExternalEventCorrel;
import little.horse.common.events.ExternalEventPayload;
import little.horse.common.events.TaskRunEndedEvent;
import little.horse.common.events.TaskRunEvent;
import little.horse.common.events.TaskRunResult;
import little.horse.common.events.TaskRunStartedEvent;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.EdgeCondition;
import little.horse.common.objects.metadata.Edge;
import little.horse.common.objects.metadata.ExceptionHandlerSpec;
import little.horse.common.objects.metadata.InterruptDef;
import little.horse.common.objects.metadata.Node;
import little.horse.common.objects.metadata.NodeType;
import little.horse.common.objects.metadata.ThreadSpec;
import little.horse.common.objects.metadata.VariableAssignment;
import little.horse.common.objects.metadata.VariableMutationOperation;
import little.horse.common.objects.metadata.VariableMutation;
import little.horse.common.objects.metadata.WFRunVariableDef;
import little.horse.common.objects.metadata.WFRunVariableTypeEnum;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.LHUtil;


@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "id"
)
public class ThreadRun extends BaseSchema {
    public String threadSpecName;

    @JsonBackReference
    public WFRun wfRun;

    @JsonManagedReference
    public ArrayList<TaskRun> taskRuns;
    public ArrayList<Edge> upNext;
    public LHExecutionStatus status;

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
    private ThreadSpec privateThreadSpec;

    @JsonIgnore
    public ThreadSpec threadSpec;

    @JsonIgnore
    private ThreadSpec getThreadSpec() throws LHConnectionError {
        return wfRun.getWFSpec().threadSpecs.get(threadSpecName);
    }

    @Override
    public String getId() {
        return String.valueOf(id);
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
            ThreadRun parent = wfRun.threadRuns.get(parentThreadID);
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
        WFSpec wfSpec = wfRun.getWFSpec();
        ThreadSpec threadSchema = wfSpec.threadSpecs.get(threadSpecName);

        WFRunVariableDef varDef = threadSchema.variableDefs.get(varName);
        if (varDef != null) {
            return new VariableLookupResult(varDef, this, variables.get(varName));
        }

        if (parentThreadID != null) {
            ThreadRun parent = wfRun.threadRuns.get(parentThreadID);
            return parent.getVariableDefinition(varName);
        }

        // out of luck
        return null;
    }

    @JsonIgnore
    public Object getMutationRHS(
        VariableMutation mutSchema, TaskRun tr
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
    public Object assignVariable(VariableAssignment var)
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
                return wfRun.id;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.WF_SPEC_GUID) {
                return wfRun.wfSpecDigest;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.WF_SPEC_NAME) {
                return wfRun.wfSpecName;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.THREAD_GUID) {
                return String.valueOf(this.id) + "-"+ wfRun.id;
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
    public void addEdgeToUpNext(Edge edge) {
        upNext.add(edge);
    }

    @JsonIgnore
    public TaskRun createNewTaskRun(Node node) 
    throws LHConnectionError {
        TaskRun tr = new TaskRun();
        tr.status = LHExecutionStatus.RUNNING;
        tr.threadID = id;
        tr.number = taskRuns.size();
        tr.nodeName = node.name;
        tr.wfSpecDigest = wfRun.getWFSpec().getId();
        tr.wfSpecName = wfRun.getWFSpec().name;

        tr.parentThread = this;

        return tr;
    }

    @JsonIgnore
    public void incorporateEvent(WFEvent wfEvent)
    throws LHConnectionError {
        TaskRunEvent event;
        try {
            event = BaseSchema.fromString(
                wfEvent.content, TaskRunEvent.class, config
            );
        } catch (LHSerdeError exn) {
            throw new RuntimeException("Not possible");
        }
        if (event.startedEvent != null) {
            handleTaskStarted(event);
        } else if (event.endedEvent != null) {
            handleTaskEnded(event);
        }
    }

    // Potentially this and handleTaskEnded could go in the TaskRunSchema.java,
    // but I'm not sure I wanna deal with jumping back and forth like that.
    @JsonIgnore
    private void handleTaskStarted(TaskRunEvent trEvent) {
        TaskRun tr = taskRuns.get(trEvent.taskRunNumber);
        TaskRunStartedEvent event = trEvent.startedEvent;

        tr.status = LHExecutionStatus.RUNNING;
        tr.startTime = trEvent.timestamp;
        tr.bashCommand = event.bashCommand;
        tr.stdin = event.stdin;
    }

    @JsonIgnore
    public WFEvent newWFEvent(WFEventType type, BaseSchema content) {
        WFEvent out = wfRun.newWFEvent(type, content);
        out.threadID = id;
        return out;
    }

    @JsonIgnore
    private void completeTask(
        TaskRun task,
        LHExecutionStatus taskStatus,
        TaskRunResult result,
        Date endTime
    ) throws LHConnectionError {
        String stdout = result.stdout;
        String stderr = result.stderr;
        int returnCode = result.returncode;
        task.endTime = endTime;
        task.stdout = LHUtil.jsonifyIfPossible(stdout, config);
        task.stderr = LHUtil.jsonifyIfPossible(stderr, config);
        task.status = taskStatus;
        task.returnCode = returnCode;

        unlockVariables(task.getNode());

        // Need the up next to be set whether or not the task fails/there is
        // a retry/it succeeds.
        upNext = new ArrayList<Edge>();
        for (Edge edge: task.getNode().getOutgoingEdges()) {
            upNext.add(edge);
        }

        if (taskStatus == LHExecutionStatus.COMPLETED) {
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
    private void handleTaskEnded(TaskRunEvent trEvent)
    throws LHConnectionError {
        TaskRun tr = taskRuns.get(trEvent.taskRunNumber);
        TaskRunEndedEvent event = trEvent.endedEvent;
        LHExecutionStatus taskStatus = event.result.success
            ? LHExecutionStatus.COMPLETED : LHExecutionStatus.FAILED;

        completeTask(
            tr, taskStatus, event.result, trEvent.timestamp
        );
    }

    @JsonIgnore
    public void mutateVariables(TaskRun tr) 
    throws VarSubOrzDash, LHConnectionError {

        // We need to do this atomically—-i.e. if there's one variable substitution
        // failure, none of the variables get mutated at all. Therefore we compute
        // all of the new values first and then assign them later once we're sure
        // there are no VarSubOrzDash's.
        ArrayList<Mutation> mutations = new ArrayList<Mutation>();

        for (Map.Entry<String, VariableMutation> pair:
            tr.getNode().variableMutations.entrySet())
        {
            String varName = pair.getKey();
            VariableMutation mutSchema = pair.getValue();
            VariableLookupResult varLookup = getVariableDefinition(varName);

            WFRunVariableDef varDef = varLookup.varDef;
            ThreadRun thread = varLookup.thread;
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
        String handlerSpecName, TaskRun tr, LHFailureReason reason, String msg
    ) throws LHConnectionError {
        tr.status = LHExecutionStatus.FAILED;
        tr.failureMessage = msg;
        tr.failureReason = reason;

        ThreadRun handler = wfRun.createThreadClientAdds(
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
        TaskRun tr, LHFailureReason reason, String message
    ) throws LHConnectionError {
        tr.status = LHExecutionStatus.FAILED;
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
    boolean evaluateEdge(EdgeCondition condition)
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

    public void updateStatus() {
        if (isCompleted()) return;
        if (upNext == null) upNext = new ArrayList<Edge>();

        if (status == LHExecutionStatus.RUNNING) {
            // If there are no pending taskruns and the last one executed was
            // COMPLETED, then the thread is now completed.
            if (upNext == null || upNext.size() == 0) {
                TaskRun lastTr = taskRuns.size() > 0 ?
                    taskRuns.get(taskRuns.size() - 1) : null;
                if (lastTr == null || lastTr.isCompleted()) {
                    status = LHExecutionStatus.COMPLETED;
                }
            } else {
                if (taskRuns.size() > 0) {
                    TaskRun lastTr = taskRuns.get(taskRuns.size() - 1);
                    if (lastTr.status == LHExecutionStatus.FAILED) {
                        status = LHExecutionStatus.HALTED;
                    }
                }
            }

        } else if (status == LHExecutionStatus.HALTED) {
            // Check if interrupt handlers are done now (:
            for (int i = activeInterruptThreadIDs.size() - 1; i >= 0; i--) {
                int tid = activeInterruptThreadIDs.get(i);
                if (tid >= wfRun.threadRuns.size()) continue;

                ThreadRun intHandler = wfRun.threadRuns.get(tid);
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
                ThreadRun handler = wfRun.threadRuns.get(
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
        } else if (status == LHExecutionStatus.HALTING) {
            // Well we just gotta see if the last task run is done.
            if (taskRuns.size() == 0 || taskRuns.get(
                    taskRuns.size() - 1
            ).isTerminated()) {
                status = LHExecutionStatus.HALTED;
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
    public boolean lockVariables(Node n, int threadID) {
        HashSet<String> neededVars = n.getNeededVars();

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
    public void unlockVariables(Node n) {
        for (String var: n.getNeededVars()) {
            unlock(var);
        }
    }

    @JsonIgnore
    public boolean advance(WFEvent event, ArrayList<TaskScheduleRequest> toSchedule)
    throws LHConnectionError {
        if (status != LHExecutionStatus.RUNNING || upNext.size() == 0) {
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
        Node activatedNode = null;
        for (Edge edge: upNext) {
            try {
                if (evaluateEdge(edge.condition)) {
                    Node n = getThreadSpec().nodes.get(edge.sinkNodeName);
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
                TaskRun lastTr = taskRuns.get(taskRuns.size() - 1);
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
            upNext = new ArrayList<Edge>();
            return true;
        }

        if (activatedNode == null && !shouldClear) {
            // then we're blocked but nothing changed.
            return false;
        }

        return activateNode(activatedNode, event, toSchedule);
    }

    @JsonIgnore
    private void scheduleTask(
        TaskRun tr, Node node, ArrayList<TaskScheduleRequest> toSchedule
    ) {
        TaskScheduleRequest te = new TaskScheduleRequest();
        te.setConfig(config);
        te.taskQueueName = node.taskDef.taskQueueName;
        te.wfRunId = wfRun.id;
        te.wfSpecId = wfRun.wfSpecDigest;
        te.wfSpecName = wfRun.wfSpecName;
        te.threadRunNumber = id;
        te.taskRunNumber = tr.number;
        try {
            te.taskDefName = tr.getNode().taskDefName;
            te.taskDefId = tr.getNode().taskDefId;
        } catch(LHConnectionError exn) {
            throw new RuntimeException(
                "Shouldn't happen because we should have already loaded the wfspec"
            );
        }

        toSchedule.add(te);
    }

    @JsonIgnore
    private boolean activateNode(
        Node node, WFEvent event, ArrayList<TaskScheduleRequest> toSchedule
    ) throws LHConnectionError {
        if (node.nodeType == NodeType.TASK) {
            upNext = new ArrayList<Edge>();
            TaskRun tr = createNewTaskRun(node);
            taskRuns.add(tr);

            scheduleTask(tr, node, toSchedule);
            return true;

        } else if (node.nodeType == NodeType.EXTERNAL_EVENT) {
            ArrayList<ExternalEventCorrel> relevantEvents =
            wfRun.correlatedEvents.get(node.externalEventDefName);
            if (relevantEvents == null) {
                relevantEvents = new ArrayList<ExternalEventCorrel>();
                wfRun.correlatedEvents.put(node.externalEventDefName, relevantEvents);
            }
            ExternalEventCorrel correlSchema = null;
            
            for (ExternalEventCorrel candidate : relevantEvents) {
                // In the future, we may want to add the ability to signal
                // a specific thread rather than the whole wfRun. We would do that here.
                if (candidate.event != null && candidate.assignedNodeName== null) {
                    correlSchema = candidate;
                }
            }
            if (correlSchema == null) return false;  // Still waiting nothing changed

            TaskRun tr = createNewTaskRun(node);
            taskRuns.add(tr);
            correlSchema.assignedNodeName = node.name;
            correlSchema.assignedTaskRunExecutionNumber = tr.number;
            correlSchema.assignedThreadID = tr.threadID;

            TaskRunResult result = new TaskRunResult(
                correlSchema.event.content.toString(), null, true, 0
            );

            completeTask(
                tr, LHExecutionStatus.COMPLETED, result, correlSchema.event.timestamp
            );
            upNext = new ArrayList<Edge>();
            return true; // Obviously something changed, we done did add a task.

        } else if (node.nodeType == NodeType.SPAWN_THREAD) {
            upNext = new ArrayList<Edge>();
            HashMap<String, Object> inputVars = new HashMap<String, Object>();
            TaskRun tr = createNewTaskRun(node);
            try {
                for (Map.Entry<String, VariableAssignment> pair:
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

            ThreadRun thread = wfRun.createThreadClientAdds(
                node.threadSpawnThreadSpecName, inputVars, this
            );
            wfRun.threadRuns.add(thread);
    
            if (wfRun.awaitableThreads.get(tr.nodeName) == null) {
                wfRun.awaitableThreads.put(
                    tr.nodeName, new ArrayList<ThreadRunMeta>()
                );
            }

            ThreadRunMeta meta = new ThreadRunMeta(tr, thread);
            wfRun.awaitableThreads.get(tr.nodeName).add(meta);
            taskRuns.add(tr);
            TaskRunResult result = new TaskRunResult(meta.toString(), null, true, 0);
            completeTask(
                tr, LHExecutionStatus.COMPLETED, result, event.timestamp
            );
            return true;

        } else if (node.nodeType == NodeType.WAIT_FOR_THREAD) {
            return handleWaitForThreadNode(node, event);
        } else if (node.nodeType == NodeType.THROW_EXCEPTION) {
            TaskRun tr = createNewTaskRun(node);
            taskRuns.add(tr);
            exceptionName = node.exceptionToThrow;

            TaskRunResult result = new TaskRunResult(
                null, "Throwing exception " + exceptionName, false, -1
            );
            completeTask(tr, LHExecutionStatus.FAILED, result, event.timestamp);
            return true;
        }
        throw new RuntimeException("invalid node type: " + node.nodeType);
    }

    private boolean handleWaitForThreadNode(
        Node node, WFEvent event
    ) throws LHConnectionError {
        // Iterate through all of the ThreadRunMetaSchema's in the wfRun.
        // If it's from the node we're waiting for and it's NOT done, then
        // this node does nothing. But if it's already done, we mark it as
        // awaited and continue on. If all of the relevant threads are done,
        // then this node completes.
        TaskRun tr = createNewTaskRun(node);
        ArrayList<ThreadRunMeta> awaitables = wfRun.awaitableThreads.get(
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

        for (ThreadRunMeta meta: awaitables) {
            ThreadRun thread = wfRun.threadRuns.get(meta.threadID);
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
                tr, LHExecutionStatus.COMPLETED,
                new TaskRunResult(awaitables.toString(), null, true, 0), event.timestamp
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

            ThreadRun thread = wfRun.threadRuns.get(awaitables.get(0).threadID);
            if (thread.isCompleted() || !thread.isTerminated()) {
                throw new RuntimeException("should be impossible");
            }

            ExceptionHandlerSpec hspec = node.getHandlerSpec(
                thread.exceptionName
            );
            if (hspec == null) {
                throw new RuntimeException(
                    "Colt, you need to address case where there is no handler.");
            } else {
                String msg = "TaskRun on " + tr.nodeName +
                " Failed with exception " + hspec.handlerThreadSpecName + ", so" +
                " we are handling it.";

                completeTask(
                    tr, LHExecutionStatus.FAILED, new TaskRunResult(
                        awaitables.toString(), msg, false, 1
                    ), event.timestamp
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
    public ArrayList<ThreadRun> getChildren() {
        ArrayList<ThreadRun> out = new ArrayList<ThreadRun>();
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
        if (status == LHExecutionStatus.RUNNING) {
            status = LHExecutionStatus.HALTING;
            errorMessage += message + "\n";
        } else if (isCompleted()) {
            LHUtil.log(
                "Somehow we find ourself in the halt() method on a completed",
                "thread, which might mean that this is a child thread who has",
                "already returned back to his dad."
            );
        }
        haltReasons.add(reason);

        for (ThreadRun kid: getChildren()) {
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
            if (status == LHExecutionStatus.HALTED || status == LHExecutionStatus.HALTING) {
                status = LHExecutionStatus.RUNNING;
                errorMessage = "";
            }

            for (ThreadRun kid: getChildren()) {
                kid.removeHaltReason(WFHaltReasonEnum.PARENT_STOPPED);
            }
        } else if (haltReasons.size() == 1
            && haltReasons.contains(WFHaltReasonEnum.INTERRUPT)
        ) {
            // In this case, the only thing holding up the parent is one (or more)
            // interrupt threads. Those threads shouldn't be blocked by the parent
            // at this point, so we unblock them.
            for (ThreadRun kid: getChildren()) {
                // Only unblock the interrupts!!!
                if (kid.isInterruptThread) {
                    kid.removeHaltReason(WFHaltReasonEnum.PARENT_INTERRUPTED);
                }
            }
        }
    }

    @JsonIgnore
    public void handleInterrupt(ExternalEventPayload payload) throws
    LHConnectionError {

        HashMap<String, InterruptDef> idefs = getThreadSpec().interruptDefs;
        InterruptDef idef = idefs.get(payload.externalEventDefName);
        String tspecname = idef.handlerThreadName;

        // crucial to create the thread BEFORE calling halt(), as the call to halt()
        // adds a WFHaltReason which we dont wanna propagate to the interrupt thread.
        ThreadRun trun = wfRun.createThreadClientAdds(
            tspecname,
            LHUtil.unsplat(payload.content, config),
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
        return (status == LHExecutionStatus.HALTED && haltReasons.contains(
            WFHaltReasonEnum.FAILED
        ));
    }

    @JsonIgnore
    public boolean isCompleted() {
        return (status == LHExecutionStatus.COMPLETED);
    }

    @JsonIgnore
    public boolean isTerminated() {
        return (isCompleted() || haltReasons.contains(WFHaltReasonEnum.FAILED));
    }

    @JsonIgnore
    public void propagateInterrupt(ExternalEventPayload payload) throws
    LHConnectionError {
        HashMap<String, InterruptDef> idefs = getThreadSpec().interruptDefs;
        if (idefs != null && idefs.containsKey(payload.externalEventDefName)) {
            // Now we need to add thread!
            handleInterrupt(payload);
        } else {
            for (ThreadRun kid: getChildren()) {
                kid.propagateInterrupt(payload);
            }
        }
    }
}


class Mutation {
    public Object lhs;
    public Object rhs;
    public VariableMutationOperation op;
    public ThreadRun tr;
    public WFRunVariableDef varDef;
    public String varName;

    public Mutation(
        Object lhs, Object rhs, VariableMutationOperation op, ThreadRun tr,
        WFRunVariableDef varDef, String varName
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

    /**
     * Used to evaluate Workflow Conditional Branching Expressions. The Left is an
     * object of some sort, and the right is another. Returns true if the Left
     * has the Right inside it.
     * @param left haystack
     * @param right needle
     * @return true if haystack has the needle in it.
     * @throws VarSubOrzDash if we can't cast the left to a container of objects,
     * or if we get an exception while comparing the equality of two things inside
     * right.
     */
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
