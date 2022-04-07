package little.horse.common.objects.rundata;

import little.horse.common.objects.BaseSchema;

public class WFRunTimer extends BaseSchema {
    public String wfRunId;
    public int threadRunId;
    public int taskRunId;
    public String nodeName;
    public long maturationTimestamp;
}
