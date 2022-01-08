package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.Date;

import little.horse.lib.LHFailureReason;
import little.horse.lib.LHStatus;

public class TaskRunSchema extends BaseSchema {
    public int number;
    public String wfSpecGuid;
    public String wfSpecName;
    public int tokenNumber;

    public String nodeName;
    public String nodeGuid;

    public ArrayList<String> bashCommand;
    public Object stdin;
    
    public LHStatus status;
    public Object stdout;
    public Object stderr;
    public int returnCode;

    public Date startTime;
    public Date endTime;

    public LHFailureReason failureReason;
    public String failureMessage;
}
