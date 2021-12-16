package little.horse.lib.schemas;

import little.horse.lib.LHFailureReason;

public class TaskRunFailedEventSchema extends TaskRunEndedEventSchema {
    public LHFailureReason reason;
    public String message;
}
