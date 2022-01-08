package little.horse.lib.schemas;

public class VariableAssignmentSchema extends BaseSchema {
    public String nodeName;
    public String wfRunVariableName;
    public String jsonPath;
    public Object literalValue;
    public boolean useLatestTaskRun = true;
    public Object defaultValue;
    public WFRunMetadataEnum wfRunMetadata;
}
