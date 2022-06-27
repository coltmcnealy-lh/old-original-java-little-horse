package io.littlehorse.common.objects.rundata;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.Node;
import io.littlehorse.common.objects.metadata.NodeType;

public class TaskRun extends BaseSchema {
    @JsonBackReference
    public ThreadRun parentThread;

    public int position;
    public int number;
    public String wfSpecId;
    public String wfSpecName;
    public int threadId;

    public String nodeName;
    public String nodeId;

    public int taskDefVersionNumber;

    public String workerId;
    public Object stdin;
    public int attemptNumber = 0;

    public LHExecutionStatus status;
    public Object stdout;
    public Object stderr;
    public int returnCode;

    public Date scheduleTime;
    public Date startTime;
    public Date endTime;

    public LHFailureReason failureReason;
    public String failureMessage;

    public String taskExecutionGuid;

    @JsonIgnore
    public boolean isTerminated() {
        return (
            status == LHExecutionStatus.COMPLETED
            || status == LHExecutionStatus.HALTED
        );
    }

    @JsonIgnore
    public boolean isCompleted() {
        return (
            status == LHExecutionStatus.COMPLETED
            || status == LHExecutionStatus.HALTED
        );
    }

    @JsonIgnore
    public Node getNode() throws LHConnectionError {
        if (parentThread == null) {
            throw new RuntimeException("Parent thread of taskrun was null!");
        }
        return parentThread.wfRun.getWFSpec().findThreadSpec(
            parentThread.threadSpecName
        ).findNode(nodeName);
    }

    public NodeType getNodeType() throws LHConnectionError {
        return getNode().nodeType;
    }
    // Just here for Jackson
    public void setNodeType(NodeType foo) {}
}
