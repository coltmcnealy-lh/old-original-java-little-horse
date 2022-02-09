package little.horse.common.events;

import java.util.Date;

import little.horse.common.objects.BaseSchema;

public class TaskRunEvent extends BaseSchema {
    public int threadID;
    public int taskRunNumber;
    public Date timestamp;

    // Only one of the below fields can be non-null.
    public TaskRunStartedEvent startedEvent;
    public TaskRunEndedEvent endedEvent;
}
