package little.horse.lib.schemas;

public class VariableAssignmentSchema extends BaseSchema {
    // A Task-level variable can be assigned to one of three things:
    // 1. A WFRunVariable
    // 2. A literal value
    // 3. Something from the metadata of the ThreadRun or WFRun.
    public String wfRunVariableName;
    public Object literalValue;
    public WFRunMetadataEnum wfRunMetadata;

    // It can also be jsonpath'ed
    public String jsonPath;

    // And you can provide a default (:
    public Object defaultValue;
}
