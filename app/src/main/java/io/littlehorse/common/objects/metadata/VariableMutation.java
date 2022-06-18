package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.littlehorse.common.objects.BaseSchema;

public class VariableMutation extends BaseSchema {
    public VariableMutationOperation operation;
    
    /**
     * A node that mutates a variable may calculate its RHS variable in any of
     * the following ways:
     * 1. With the stdout of the task run itself or the external event.
     * 2. With another variable (i.e. by adding two variables together, etc.)
     * 3. With a literal value.
     * 
     * If `jsonpath` is not null, then the RHS var comes from the output of the node.
     * Else if `sourceVariable` is not null, then the RHS comes from the provided
     * VariableAssignmentSchema. Else, the RHS is set to the provided literal
     * value.
     */
    public String jsonPath;
    public VariableValue literalValue;
    public VariableAssignment sourceVariable;

    @JsonBackReference
    public Node node;
}
