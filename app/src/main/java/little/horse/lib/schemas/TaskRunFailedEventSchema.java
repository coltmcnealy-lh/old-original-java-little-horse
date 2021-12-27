package little.horse.lib.schemas;

import little.horse.lib.LHFailureReason;

public class TaskRunFailedEventSchema extends NodeCompletedEventSchema {
    public LHFailureReason reason;
    public String message;
}
