package little.horse.lib.schemas;

import java.util.ArrayList;

import little.horse.lib.LHFailureReason;

public class TaskRunEndedEventSchema extends TaskRunEventSchema {
    public String stdout;
    public String stderr;
    public int returncode;
    public String nodeGuid;
    public ArrayList<String> bashCommand;
    public boolean success;
    public LHFailureReason reason;
    public String message;
}
