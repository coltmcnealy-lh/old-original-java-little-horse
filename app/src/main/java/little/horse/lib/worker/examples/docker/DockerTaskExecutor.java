package little.horse.lib.worker.examples.docker;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.common.Config;
import little.horse.lib.deployers.examples.docker.DDConfig;
import little.horse.lib.worker.WorkerContext;

public interface DockerTaskExecutor {
    public void init(DDConfig ddConfig, Config config);

    public Object executeTask(
        TaskScheduleRequest request, WorkerContext context
    ) throws Exception;
}
