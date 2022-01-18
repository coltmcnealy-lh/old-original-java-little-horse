package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import org.apache.commons.lang3.tuple.Pair;
import little.horse.lib.LHFailureReason;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.VarSubOrzDash;
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
    private ThreadSpecSchema getThreadSpec()
    throws LHNoConfigException, LHLookupException {
        if (wfRun == null) {
            throw new LHNoConfigException(
                "parent wfRun isn't set and config isn't set!!"
            );
        }
        return wfRun.getWFSpec().threadSpecs.get(threadSpecName);
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

    public Pair<Object, ThreadRunSchema> getVariableValue(String varName) {
        
        return null;
    }

    public Object getMutationRHS(VariableMutationSchema mutSchema) {
        return null;
    }

    /*
    commented out because we're going to replace upNext with edges rather than
    taskRuns to avoid the following race condition:
      - task A is scheduled after an edge fires with condition (myVar > 0)
      - task A is delayed because it contends with task B to edit variable foo.
      - task B completes and mutates myVar so now myVar < 0
      - task A now is unblocked and executes, even though myVar < 0. Orz.
    */
    // public void addTaskRunToUpNext(NodeSchema node)
    // throws LHNoConfigException, LHLookupException {
    //     TaskRunSchema tr = new TaskRunSchema();
    //     tr.status = LHStatus.PENDING;
    //     tr.threadID = id;
    //     tr.number = taskRuns.size();
    //     tr.nodeName = node.name;
    //     tr.nodeGuid = node.guid;
    //     tr.wfSpecGuid = wfRun.getWFSpec().guid;
    //     tr.wfSpecName = wfRun.getWFSpec().name;

    //     upNext.add(tr);
    // }

    public void incorporateEvent(WFEventSchema wfEvent)
    throws LHLookupException, LHNoConfigException {
        TaskRunEventSchema event = BaseSchema.fromString(
            wfEvent.toString(), TaskRunEventSchema.class
        );
        if (event.startedEvent != null) {
            handleTaskStarted(event);
        } else if (event.endedEvent != null) {
            handleTaskEnded(event);
        }
    }

    // Potentially this and handleTaskEnded could go in the TaskRunSchema.java,
    // but I'm not sure I wanna deal with jumping back and forth like that.
    private void handleTaskStarted(TaskRunEventSchema trEvent) {
        TaskRunSchema tr = taskRuns.get(trEvent.taskRunNumber);
        TaskRunStartedEventSchema event = trEvent.startedEvent;

        tr.status = LHStatus.RUNNING;
        tr.startTime = trEvent.timestamp;
        tr.bashCommand = event.bashCommand;
        tr.stdin = event.stdin;
    }

    private void handleTaskEnded(TaskRunEventSchema trEvent)
    throws LHLookupException, LHNoConfigException {
        TaskRunSchema tr = taskRuns.get(trEvent.taskRunNumber);
        TaskRunEndedEventSchema event = trEvent.endedEvent;
        tr.endTime = trEvent.timestamp;

        tr.stdout = event.stdout;
        tr.stderr = event.stderr;
        tr.returnCode = event.returncode;
        tr.endTime = trEvent.timestamp;

        if (trEvent.endedEvent.success) {
            tr.status = LHStatus.COMPLETED;
            try {
                mutateVariables(tr);
            } catch(VarSubOrzDash exn) {
                markTaskFailed(
                    tr, LHFailureReason.VARIABLE_LOOKUP_ERROR,exn.getMessage()
                );
                return;
            }
        } else {
            tr.status = LHStatus.ERROR;
            // TODO: Call failure somewhere...
            markTaskFailed(
                tr, LHFailureReason.TASK_FAILURE,
                "thread failed on node " + tr.nodeName
            );
        }
    }

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
            Object rhs = getMutationRHS(mutSchema);
            VariableMutationOperation op = mutSchema.operation;

            // Ok, if we got this far, then we know that the RHS and LHS both exist,
            // but are LHS + operation + RHS valid?
            Mutation mut = new Mutation(lhs, rhs, op, thread, varDef, varName);
            mut.checkValidity();
            mutations.add(mut);
        }

        // If we've gotten this far, then we know (if I coded everything properly)
        // that we aren't going to have an error when we finally apply the
        // mutations.
        for (Mutation mutation: mutations) {
            mutation.execute();
        }
    }

    private void markTaskFailed(
        TaskRunSchema tr, LHFailureReason reason, String message
    ) {
        fail(LHFailureReason.TASK_FAILURE, "oops");
        throw new RuntimeException("Implement me");
    }

    private void fail(LHFailureReason reason, String message) {
        throw new RuntimeException("Implement me");
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

    @SuppressWarnings("unused")
    public void checkValidity() throws VarSubOrzDash {
        Class<?> defTypeCls = null;
        switch (varDef.type) {
            case STRING: defTypeCls = String.class; break;
            case OBJECT: defTypeCls = Map.class; break;
            case INT: defTypeCls = Integer.class; break;
            case DOUBLE: defTypeCls = Double.class; break;
            case BOOLEAN: defTypeCls = Boolean.class; break;
            case ARRAY: defTypeCls = List.class; break;
        }

        if (lhs != null && !defTypeCls.isInstance(lhs)) {
            throw new VarSubOrzDash(
                null, "Variable " + varName + " is not of the correct type "
                + defTypeCls.getName() + ", it is a " + lhs.getClass().getName()
            );
        }

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
                    Integer toAdd = (Integer) rhs;
                } else if (varDef.type == WFRunVariableTypeEnum.STRING) {
                    String toAdd = (String) rhs;
                } else if (varDef.type == WFRunVariableTypeEnum.ARRAY) {
                    // nothing to do here until we start enforcing json schemas
                    // within arrays
                } else if (varDef.type == WFRunVariableTypeEnum.DOUBLE) {
                    Double toAdd = (Double) rhs;
                }
            } catch(Exception exn) {
                throw new VarSubOrzDash(exn,
                    "Failed casting the value " + rhs.toString() + " to a " +
                    defTypeCls.getName()
                );
            }


        }

    }

    public void execute() {

    }

    @SuppressWarnings("all") // lol
    private static int compare(Object left, Object right) throws VarSubOrzDash {

        LHUtil.log("Left class: ", left.getClass());
        LHUtil.log("right class: ", right.getClass());
        try {
            LHUtil.log("Comparing", left, "to", right);
            int result = ((Comparable) left).compareTo((Comparable) right);
            LHUtil.log("got:", result);
            return result;
        } catch(Exception exn) {
            LHUtil.logError(exn.getMessage());
            throw new VarSubOrzDash(exn, "Failed comparing the provided values.");
        }
    }

    @SuppressWarnings("all")
    private static boolean contains(Object left, Object right) throws VarSubOrzDash {
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
}
