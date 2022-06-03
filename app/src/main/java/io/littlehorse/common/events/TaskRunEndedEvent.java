package io.littlehorse.common.events;


import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.rundata.LHFailureReason;

public class TaskRunEndedEvent extends BaseSchema {
    public TaskRunResult result;
    public int threadId;
    public int taskRunPosition;
    public LHFailureReason reason;
    public String message;
}
