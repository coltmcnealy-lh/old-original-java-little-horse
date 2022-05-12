package little.horse.common.events;


import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHFailureReason;

public class TaskRunEndedEvent extends BaseSchema {
    public TaskRunResult result;
    public int threadId;
    public int taskRunPosition;
    public LHFailureReason reason;
    public String message;
}
