package io.littlehorse.common.events;


import io.littlehorse.common.objects.BaseSchema;

public class TaskRunStartedEvent extends BaseSchema {
    public String workerId;
    public String stdin;
    public String nodeName;
    public int taskRunPosition;
    public int threadId;
}
