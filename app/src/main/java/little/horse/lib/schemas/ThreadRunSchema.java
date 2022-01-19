package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import little.horse.lib.Config;
import little.horse.lib.LHFailureReason;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.NodeType;
import little.horse.lib.VarSubOrzDash;
import little.horse.lib.WFEventProcessorActor;
import little.horse.lib.WFEventType;
import little.horse.lib.wfRuntime.WFRunStatus;

public class ThreadRunSchema extends BaseSchema {
    public String threadSpecName;
    public String threadSpecGuid;

    public ArrayList<EdgeSchema> upNext;

    @JsonManagedReference
    public ArrayList<TaskRunSchema> taskRuns;

    public HashMap<String, Object> variables;

    public WFRunStatus status;
    public LHFailureReason errorCode;
    public String errorMessage;

    public int id;
    public Integer parentThreadID;
    public ArrayList<Integer> childThreadIDs;

    @JsonIgnore
    private ThreadSpecSchema privateThreadSpec;

    @JsonBackReference
    public WFRunSchema wfRun;

    @JsonIgnore
    public ThreadSpecSchema threadSpec;

    @JsonIgnore
    private ThreadSpecSchema getThreadSpec()
    throws LHNoConfigException, LHLookupException {
        if (threadSpec != null) return threadSpec;
        if (wfRun == null) {
            throw new LHNoConfigException(
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
    public HashMap<String, Object> getAllVariables() throws LHNoConfigException {
        if (wfRun == null) {
            throw new LHNoConfigException(
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
    ) throws LHNoConfigException, LHLookupException {
        if (wfRun == null) {
            throw new LHNoConfigException(
                "wfRun was not set yet!"
            );
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
    ) throws LHNoConfigException, LHLookupException, VarSubOrzDash {
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
    throws LHNoConfigException, LHLookupException, VarSubOrzDash {
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
    throws LHNoConfigException, LHLookupException {
        TaskRunSchema tr = new TaskRunSchema();
        tr.status = LHStatus.PENDING;
        tr.threadID = id;
        tr.number = taskRuns.size();
        tr.nodeName = node.name;
        tr.nodeGuid = node.guid;
        tr.wfSpecGuid = wfRun.getWFSpec().guid;
        tr.wfSpecName = wfRun.getWFSpec().name;

        tr.parentThread = this;

        return tr;
    }

    @JsonIgnore
    public void incorporateEvent(WFEventSchema wfEvent)
    throws LHLookupException, LHNoConfigException {
        TaskRunEventSchema event = BaseSchema.fromString(
            wfEvent.content, TaskRunEventSchema.class
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
    ) throws LHLookupException, LHNoConfigException {
        task.endTime = endTime;
        task.stdout = LHUtil.jsonifyIfPossible(stdout);
        task.stderr = LHUtil.jsonifyIfPossible(stderr);
        task.status = taskStatus;
        task.returnCode = returnCode;

        wfRun.unlockVariables(task.getNode());

        if (taskStatus == LHStatus.COMPLETED) {
            try {
                mutateVariables(task);
            } catch(VarSubOrzDash exn) {
                markTaskFailed(
                    task, LHFailureReason.VARIABLE_LOOKUP_ERROR,exn.getMessage()
                );
                return;
            }

            // now schedule the up next (:
            for (EdgeSchema edge: task.getNode().outgoingEdges) {
                upNext.add(edge);
            }

        } else {
            markTaskFailed(
                task, LHFailureReason.TASK_FAILURE,
                "thread failed on node " + task.nodeName
            );
        }
    }

    @JsonIgnore
    private void handleTaskEnded(TaskRunEventSchema trEvent)
    throws LHLookupException, LHNoConfigException {
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
    throws VarSubOrzDash, LHLookupException, LHNoConfigException {

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
    private void markTaskFailed(
        TaskRunSchema tr, LHFailureReason reason, String message
    ) {
        fail(LHFailureReason.TASK_FAILURE, "oops");
        throw new RuntimeException("Implement me");
    }

    @JsonIgnore
    private void fail(LHFailureReason reason, String message) {
        throw new RuntimeException("Implement me");
    }

    @JsonIgnore
    boolean evaluateEdge(EdgeConditionSchema condition)
    throws VarSubOrzDash, LHNoConfigException, LHLookupException {
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
        if (status == WFRunStatus.COMPLETED) return;
        if (upNext == null) upNext = new ArrayList<EdgeSchema>();

        if (status == WFRunStatus.RUNNING) {
            // If there are no pending taskruns and the last one executed was
            // COMPLETED, then the thread is now completed.
            if (upNext == null || upNext.size() == 0) {
                TaskRunSchema lastTr = taskRuns.size() > 0 ?
                    taskRuns.get(taskRuns.size() - 1) : null;
                if (lastTr == null || lastTr.status == LHStatus.COMPLETED) {
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
            // This shouldn't really be possible I don't think
            LHUtil.log(
                "What? How are we getting here when the thread is already halted?"
            );
        } else if (status == WFRunStatus.HALTING) {
            // Well we just gotta see if the last task run is done.
            TaskRunSchema lastTr = taskRuns.get(taskRuns.size() - 1);
            if (lastTr.status == LHStatus.COMPLETED ||
                lastTr.status == LHStatus.ERROR
            ) {
                status = WFRunStatus.HALTED;
            }
        }
    }

    @JsonIgnore
    public boolean advance(WFEventSchema event, WFEventProcessorActor actor)
    throws LHLookupException, LHNoConfigException {
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
                    if (wfRun.lockVariables(n, id)) {
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
                markTaskFailed(
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

        return activateNode(activatedNode, actor, event);
    }

    @JsonIgnore
    private boolean activateNode(
        NodeSchema node, WFEventProcessorActor actor, WFEventSchema event)
    throws LHLookupException, LHNoConfigException {
        if (node.nodeType == NodeType.TASK) {
            upNext = new ArrayList<EdgeSchema>();
            TaskRunSchema tr = createNewTaskRun(node);
            taskRuns.add(tr);
            if (node.guid.equals(actor.getNodeGuid())) {
                actor.act(wfRun, id, tr.number);
            }
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
            correlSchema.assignedNodeGuid = node.guid;
            correlSchema.assignedNodeName = node.name;
            correlSchema.assignedTaskRunExecutionNumber = tr.number;
            correlSchema.assignedThreadID = tr.threadID;

            completeTask(
                tr, LHStatus.COMPLETED, correlSchema.event.content.toString(),
                null, correlSchema.event.timestamp, 0
            );
            upNext = new ArrayList<EdgeSchema>();
            return true; // Obviously something changed, we done did add a task.

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
                markTaskFailed(
                    tr, LHFailureReason.VARIABLE_LOOKUP_ERROR,
                    "Failed creating variables for subthread: " + exn.getMessage()
                );
                return true;
            }

            ThreadRunMetaSchema meta = wfRun.addThread(
                node.threadSpawnThreadSpecName,
                inputVars,
                WFRunStatus.RUNNING,
                tr
            );
            completeTask(
                tr, LHStatus.COMPLETED, meta.toString(), null, event.timestamp, 0
            );
            return true;

        } else if (node.nodeType == NodeType.WAIT_FOR_THREAD) {
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
                markTaskFailed(
                    tr, LHFailureReason.INVALID_WF_SPEC_ERROR,
                    "Got to node " + node.name + " which waits for a thread from " +
                    node.threadWaitSourceNodeName + " but no threads have started" +
                    " from the specified source node."
                );
                return true;
            }

            ArrayList<ThreadRunMetaSchema> waitedFor = new ArrayList<>();
            for (ThreadRunMetaSchema meta: awaitables) {
                ThreadRunSchema threadRun = wfRun.threadRuns.get(meta.threadID);
                if (meta.timesAwaited == 0 &&
                    threadRun.status == WFRunStatus.COMPLETED
                ) {
                    waitedFor.add(meta);
                } else if (threadRun.status != WFRunStatus.RUNNING) {
                    return false;
                }
            }
            if (waitedFor.size() == 0) return false;

            // If we got this far, then we know that the threads have been awaited.
            for (ThreadRunMetaSchema meta: waitedFor) {
                meta.timesAwaited++;
            }
            taskRuns.add(tr);
            upNext = new ArrayList<EdgeSchema>();
            completeTask(
                tr, LHStatus.COMPLETED, waitedFor.toString(), null, event.timestamp, 0
            );
            return true;
        }
        throw new RuntimeException("invalid node type");
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
