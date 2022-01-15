package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import little.horse.lib.LHFailureReason;
import little.horse.lib.wfRuntime.WFRunStatus;

public class ThreadRunSchema extends BaseSchema {
    public String threadSpecName;
    public String threadSpecGuid;

    public ArrayList<TaskRunSchema> upNext;
    public ArrayList<TaskRunSchema> taskRuns;
    public HashMap<String, Object> variables;
    public TaskRunSchema activeNode;

    public WFRunStatus status;
    public LHFailureReason errorCode;
    public String errorMessage;

    public int id;
    public Integer parentThreadID;
    public ArrayList<Integer> childThreadIDs;

    @JsonIgnore
    public HashMap<String, Object> getAllVariables(WFRunSchema wfRun) {
        @SuppressWarnings("unchecked")
        HashMap<String, Object> out = (HashMap<String, Object>) variables.clone();

        // Yay, recursion!
        if (parentThreadID != null) {
            ThreadRunSchema parent = wfRun.threadRuns.get(parentThreadID);
            out.putAll(parent.getAllVariables(wfRun));
        }

        return out;
    }

    /**
     * 
     * @param varName
     * @param wfRun
     * @param wfSpec
     * @return a tuple of WFRunVariableDefSchema, ThreadRunSchema: The definition
     * of the variable, and the parent ThreadRun that owns it.
     */
    @JsonIgnore
    public Pair<WFRunVariableDefSchema, ThreadRunSchema> getVariableDefinition(
        String varName, WFRunSchema wfRun, WFSpecSchema wfSpec
    ) {
        ThreadSpecSchema threadSchema = wfSpec.threadSpecs.get(threadSpecName);

        WFRunVariableDefSchema varDef = threadSchema.variableDefs.get(varName);
        if (varDef != null) {
            return new ImmutablePair<WFRunVariableDefSchema, ThreadRunSchema>(
                varDef, this
            );
        }

        if (parentThreadID != null) {
            ThreadRunSchema parent = wfRun.threadRuns.get(parentThreadID);
            return parent.getVariableDefinition(varName, wfRun, wfSpec);
        }

        // out of luck
        return null;
    }
}
