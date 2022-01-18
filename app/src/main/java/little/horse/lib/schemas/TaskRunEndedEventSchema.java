package little.horse.lib.schemas;


import little.horse.lib.LHFailureReason;

public class TaskRunEndedEventSchema extends TaskRunEventSchema {
    public String stdout;
    public String stderr;
    public int returncode;
    public String nodeGuid;
    public boolean success;
    public LHFailureReason reason;
    public String message;
}
