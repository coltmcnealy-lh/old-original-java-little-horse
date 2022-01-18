package little.horse.lib.schemas;

import java.util.Date;

public class TaskRunEventSchema extends BaseSchema {
    public int threadID;
    public int taskRunNumber;
    public Date timestamp;

    // Only one of the below fields can be non-null.
    public TaskRunStartedEventSchema startedEvent;
    public TaskRunEndedEventSchema endedEvent;
}
