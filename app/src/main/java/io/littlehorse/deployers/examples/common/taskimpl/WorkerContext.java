package io.littlehorse.deployers.examples.common.taskimpl;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.scheduler.TaskScheduleRequest;

public class WorkerContext {
    private TaskScheduleRequest tsr;
    private String stderr;

    public WorkerContext(LHConfig config, TaskScheduleRequest tsr) {
        this.tsr = tsr;
    }

    public void log(Object... things) {
        LHUtil.logBack(
            1, tsr.taskDefName, tsr.taskDefName, tsr.wfRunId, tsr.threadId,
            tsr.taskRunPosition, things
        );

        for (Object thing: things) {
            if (thing != null) {
                stderr += thing.toString() + "\n";
            } else {
                stderr += "**null**\n";
            }
        }
    }

    public String getStderr() {
        return this.stderr;
    }
}
