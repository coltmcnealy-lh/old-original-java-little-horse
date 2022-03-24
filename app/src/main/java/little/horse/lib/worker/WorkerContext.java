package little.horse.lib.worker;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.common.DepInjContext;
import little.horse.common.util.LHUtil;

public class WorkerContext {
    private TaskScheduleRequest tsr;
    private String stderr;

    public WorkerContext(DepInjContext config, TaskScheduleRequest tsr) {
        this.tsr = tsr;
    }

    public void log(Object... things) {
        LHUtil.logBack(
            1, tsr.taskDefName, tsr.taskDefName, tsr.wfRunId, tsr.threadRunNumber,
            tsr.taskRunNumber, things
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
