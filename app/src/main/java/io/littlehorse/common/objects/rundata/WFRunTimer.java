package io.littlehorse.common.objects.rundata;

import io.littlehorse.common.objects.BaseSchema;

public class WFRunTimer extends BaseSchema {
    public String wfRunId;
    public int threadRunId;
    public int taskRunId;
    public String nodeName;
    public long maturationTimestamp;
}
