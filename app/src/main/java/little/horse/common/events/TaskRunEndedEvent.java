package little.horse.common.events;


import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHFailureReason;

public class TaskRunEndedEvent extends BaseSchema {
    public String stdout;
    public String stderr;
    public int returncode;
    public String nodeGuid;
    public boolean success;
    public LHFailureReason reason;
    public String message;
}
