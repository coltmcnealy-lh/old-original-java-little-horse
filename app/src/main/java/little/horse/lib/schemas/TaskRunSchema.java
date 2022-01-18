package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.lib.LHFailureReason;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHNoConfigException;
import little.horse.lib.LHStatus;

public class TaskRunSchema extends BaseSchema {
    @JsonBackReference
    public ThreadRunSchema parentThread;

    public int number;
    public String wfSpecGuid;
    public String wfSpecName;
    public int threadID;

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

    @JsonIgnore
    public NodeSchema getNode() throws LHNoConfigException, LHLookupException {
        if (parentThread == null) {
            throw new LHNoConfigException("Parent thread of taskrun was null!");
        }
        return parentThread.wfRun.getWFSpec().threadSpecs.get(
            parentThread.threadSpecName
        ).nodes.get(nodeName);
    }
}
