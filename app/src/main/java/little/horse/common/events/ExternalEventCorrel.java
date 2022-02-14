package little.horse.common.events;

import java.util.Date;

import little.horse.common.objects.BaseSchema;

public class ExternalEventCorrel extends BaseSchema {
    public ExternalEventPayload event;
    public int assignedTaskRunExecutionNumber;
    public String assignedNodeName;
    public int assignedThreadID;
    public Date arrivalTime;
}
