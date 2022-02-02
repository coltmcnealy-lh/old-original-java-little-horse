package little.horse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.exceptions.LHLookupException;
import little.horse.common.exceptions.LHNoConfigException;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.NodeSchema;

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
    public int attemptNumber = 1;

    public LHStatus status;
    public Object stdout;
    public Object stderr;
    public int returnCode;

    public Date startTime;
    public Date endTime;

    public LHFailureReason failureReason;
    public String failureMessage;

    public String taskExecutionGuid;

    @JsonIgnore
    public boolean isTerminated() {
        return (status == LHStatus.COMPLETED || status == LHStatus.ERROR);
    }

    @JsonIgnore
    public boolean isCompleted() {
        return (
            status == LHStatus.COMPLETED || status == LHStatus.FAILED_AND_HANDLED
        );
    }

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
