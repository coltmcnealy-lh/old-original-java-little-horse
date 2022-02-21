package little.horse.lib.worker;

import little.horse.api.runtime.TaskScheduleRequest;

public interface TaskExecutor {
    public Object executeTask(TaskScheduleRequest request, WorkerContext context);
}
