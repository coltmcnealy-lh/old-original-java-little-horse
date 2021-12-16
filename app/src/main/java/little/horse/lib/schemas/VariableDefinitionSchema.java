package little.horse.lib.schemas;

public class VariableDefinitionSchema extends BaseSchema {
    public String nodeName;
    public String wfRunVariableName;
    public String jsonPath;
    public String literalValue;
    public boolean useLatestTaskRun = true;
    public String defaultValue;
}
