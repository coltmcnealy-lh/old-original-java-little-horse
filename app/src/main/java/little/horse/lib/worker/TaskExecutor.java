package little.horse.lib.worker;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.common.events.TaskRunResult;

public interface TaskExecutor {
    public TaskRunResult executeTask(TaskScheduleRequest request);
}
