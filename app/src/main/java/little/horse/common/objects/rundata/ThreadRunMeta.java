package little.horse.common.objects.rundata;

import little.horse.common.objects.BaseSchema;

public class ThreadRunMeta extends BaseSchema {
    public int threadId;
    public int parentThreadId;
    public String threadSpecName;

    public String sourceNodeName;
    public String sourceNodeId;

    public ThreadRunMeta() {

    }

    public ThreadRunMeta(TaskRun task, ThreadRun thread) {
        this.sourceNodeId = task.nodeId;
        this.sourceNodeName = task.nodeName;
        this.threadId = thread.id;
        this.parentThreadId = task.threadId;
        this.threadSpecName = task.parentThread.threadSpecName;

        thread.setConfig(config);
    }
}
