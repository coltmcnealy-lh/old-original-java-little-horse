package little.horse.lib.worker;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.common.Config;
import little.horse.lib.deployers.docker.DDConfig;

public interface TaskExecutor {
    public void init(DDConfig ddConfig, Config config);
    public Object executeTask(
        TaskScheduleRequest request, WorkerContext context
    ) throws Exception;
}
