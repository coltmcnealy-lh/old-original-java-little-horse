package little.horse.common.objects.rundata;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.Node;

public class TaskRun extends BaseSchema {
    @JsonBackReference
    public ThreadRun parentThread;

    public int number;
    public String wfSpecDigest;
    public String wfSpecName;
    public int threadID;

    public String nodeName;
    public String nodeDigest;

    public String workerId;
    public Object stdin;
    public int attemptNumber = 1;

    public LHExecutionStatus status;
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
        return (
            status == LHExecutionStatus.COMPLETED
            || status == LHExecutionStatus.FAILED
        );
    }

    @JsonIgnore
    public boolean isCompleted() {
        return (
            status == LHExecutionStatus.COMPLETED
            || status == LHExecutionStatus.FAILED
        );
    }

    @JsonIgnore
    public Node getNode() throws LHConnectionError {
        if (parentThread == null) {
            throw new RuntimeException("Parent thread of taskrun was null!");
        }
        return parentThread.wfRun.getWFSpec().threadSpecs.get(
            parentThread.threadSpecName
        ).nodes.get(nodeName);
    }
}
