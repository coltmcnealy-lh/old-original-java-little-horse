package little.horse.lib.schemas;

public class ExternalEventThingySchema extends BaseSchema {
    public ExternalEventPayloadSchema event;
    public String assignedTaskRunGuid;
    public String assignedNodeName;
    public String assignedNodeGuid;
}
