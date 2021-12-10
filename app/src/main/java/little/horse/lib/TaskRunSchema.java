package little.horse.lib;

import java.util.Date;

public class TaskRunSchema {
    public String wfSpecGuid;
    public String wfSpecName;
    public String wfNodeName;
    public String wfNodeGuid;
    public int executionNumber;
    public LHStatus status;
    public String bashCommand;
    public String dockerImage;
    public String stdin;
    public String stdout;
    public String stderr;
    public int returnCode;
    public Date startTime;
    public Date endTime;
}
