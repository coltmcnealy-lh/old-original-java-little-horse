package little.horse.lib.schemas;

public class VariableMutationSchema extends BaseSchema {
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
     * 
     * (NOTE: if you wish the RHS to be the output of a node without any jsonpath
     * manipulation, set jsonpath/literalValue/sourceVariable all to null and set
     * `copyDirectlyFromNodeOutput = true`.)
     */
    public boolean copyDirectlyFromNodeOutput = false;
    public String jsonPath;
    public Object literalValue;
    public VariableAssignmentSchema sourceVariable;
}
