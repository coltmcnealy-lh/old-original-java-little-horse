package little.horse.lib.worker;

import little.horse.common.DepInjContext;
import little.horse.common.util.LHUtil;
import little.horse.workflowworker.TaskScheduleRequest;

public class WorkerContext {
    private TaskScheduleRequest tsr;
    private String stderr;

    public WorkerContext(DepInjContext config, TaskScheduleRequest tsr) {
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
