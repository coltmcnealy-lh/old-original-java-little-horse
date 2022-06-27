package io.littlehorse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.json.JsonMapKey;

@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name",
    scope = Node.class
)
public class Node extends BaseSchema {
    @JsonMapKey
    public String name;

    public VariableAssignment timeoutSeconds;
    public int numRetries = 0;  // For Retries (:

    public NodeType nodeType;

    public ArrayList<Edge> outgoingEdges;
    public ArrayList<Edge> incomingEdges;

    @JsonManagedReference
    public HashMap<String, VariableAssignment> variables;

    @JsonBackReference
    public ThreadSpec threadSpec;

    public String externalEventDefName;
    public String externalEventDefId;

    public VariableAssignment threadWaitThreadId;
    public String threadSpawnThreadSpecName;

    @JsonManagedReference
    public HashMap<String, VariableMutation> variableMutations;

    public String taskDefName;
    public String taskDefId;

    // Ignored unless node is of nodeType THROW_EXCEPTION
    public String exceptionToThrow;

    public ExceptionHandlerSpec baseExceptionhandler;

    public List<ExceptionHandlerSpec> customExceptionHandlers;

    public ArrayList<Edge> getOutgoingEdges() {
        if (outgoingEdges == null) {
            outgoingEdges = new ArrayList<>();
            for (Edge edge: threadSpec.edges) {
                if (edge.sourceNodeName.equals(name)) {
                    outgoingEdges.add(edge);
                }
            }
        }
        return outgoingEdges;
    }

    public ArrayList<Edge> getIncomingEdges() {
        if (incomingEdges == null) {
            incomingEdges = new ArrayList<>();
            for (Edge edge: threadSpec.edges) {
                if (edge.sinkNodeName.equals(name)) {
                    incomingEdges.add(edge);
                }
            }
        }
        return incomingEdges;
    }

    // Everything below doesn't show up in json.
    @JsonIgnore
    private ExternalEventDef externalEventDef;

    @JsonIgnore
    public HashSet<String> getNeededVars() {
        HashSet<String> neededVars = new HashSet<String>();
        // first figure out which variables we need as input
        for (VariableAssignment var: this.variables.values()) {
            if (var.wfRunVariableName != null) {
                neededVars.add(var.wfRunVariableName);
            }
        }

        // Now see which variables we need as output
        for (Map.Entry<String, VariableMutation> p:
            this.variableMutations.entrySet()
        ) {
            // Add the variable that gets mutated
            neededVars.add(p.getKey());

            VariableAssignment rhsVarAssign = p.getValue().sourceVariable;
            if (rhsVarAssign != null) {
                if (rhsVarAssign.wfRunVariableName != null) {
                    neededVars.add(rhsVarAssign.wfRunVariableName);
                }
            }
        }
        return neededVars;
    }

    @JsonIgnore
    public ExternalEventDef getExeternalEventDef() throws LHConnectionError {
        throw new RuntimeException("Implement me");
    }

    @JsonIgnore
    public ExceptionHandlerSpec getHandlerSpec(String exceptionName) {
        throw new RuntimeException("Implement me");
    }

}
