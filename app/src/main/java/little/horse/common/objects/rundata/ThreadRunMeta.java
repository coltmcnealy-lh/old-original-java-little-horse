package little.horse.common.objects.rundata;

import little.horse.common.objects.BaseSchema;

public class ThreadRunMeta extends BaseSchema {
    public int threadID;
    public int parentThreadID;
    public String threadSpecName;

    public String sourceNodeName;
    public String sourceNodeGuid;

    // No info about thread status or result present here because that
    // can be looked up from the threadID without another network hop.

    public int timesAwaited = 0;


    public ThreadRunMeta() {

    }

    public ThreadRunMeta(TaskRun task, ThreadRun thread) {
        this.sourceNodeGuid = task.nodeDigest;
        this.sourceNodeName = task.nodeName;
        this.threadID = thread.id;
        this.timesAwaited = 0;
        this.parentThreadID = task.threadID;
        this.threadSpecName = task.parentThread.threadSpecName;

        thread.setConfig(config);
    }
}
