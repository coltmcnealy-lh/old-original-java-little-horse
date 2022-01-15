package little.horse.lib.schemas;

public class ExternalEventCorrelSchema extends BaseSchema {
    public ExternalEventPayloadSchema event;
    public int assignedTaskRunExecutionNumber;
    public String assignedNodeName;
    public String assignedNodeGuid;
    public int assignedThreadID;
}
