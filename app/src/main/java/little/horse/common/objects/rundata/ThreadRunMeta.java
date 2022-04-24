package little.horse.common.objects.rundata;

import little.horse.common.objects.BaseSchema;

public class ThreadRunMeta extends BaseSchema {
    public int threadId;
    public int parentThreadId;
    public String threadSpecName;

    public String sourceNodeName;
    public String sourceNodeId;

    // No info about thread status or result present here because that
    // can be looked up from the threadID without another network hop.

    public int timesAwaited = 0;


    public ThreadRunMeta() {

    }

    public ThreadRunMeta(TaskRun task, ThreadRun thread) {
        this.sourceNodeId = task.nodeId;
        this.sourceNodeName = task.nodeName;
        this.threadId = thread.id;
        this.timesAwaited = 0;
        this.parentThreadId = task.threadId;
        this.threadSpecName = task.parentThread.threadSpecName;

        thread.setConfig(config);
    }
}
