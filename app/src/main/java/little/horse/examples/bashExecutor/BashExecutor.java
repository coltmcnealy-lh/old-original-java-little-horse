package little.horse.examples.bashExecutor;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.lib.worker.TaskExecutor;
import little.horse.lib.worker.WorkerContext;

public class BashExecutor implements TaskExecutor {
    public BashExecutor() {

    }

    public Object executeTask(TaskScheduleRequest request, WorkerContext context) {
        return null;
    }


}
