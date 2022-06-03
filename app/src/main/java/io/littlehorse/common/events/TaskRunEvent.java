package io.littlehorse.common.events;

import java.util.Date;

import io.littlehorse.common.objects.BaseSchema;

public class TaskRunEvent extends BaseSchema {
    public int threadId;
    public int taskRunPosition;
    public Date timestamp;
    public int taskDefVersionNumber;

    // Only one of the below fields can be non-null.
    public TaskRunStartedEvent startedEvent;
    public TaskRunEndedEvent endedEvent;
}
