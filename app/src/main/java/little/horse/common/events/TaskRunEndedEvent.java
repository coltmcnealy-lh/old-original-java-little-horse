package little.horse.common.events;


import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHFailureReason;

public class TaskRunEndedEvent extends BaseSchema {
    public TaskRunResult result;
    public int threadRunId;
    public int taskRunNumber;
    public LHFailureReason reason;
    public String message;
}
