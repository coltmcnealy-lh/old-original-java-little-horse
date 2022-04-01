package little.horse.lib.worker.examples.docker;

import little.horse.common.DepInjContext;
import little.horse.lib.deployers.examples.docker.DDConfig;
import little.horse.lib.worker.WorkerContext;
import little.horse.workflowworker.TaskScheduleRequest;

public interface DockerTaskExecutor {
    public void init(DDConfig ddConfig, DepInjContext config);

    public Object executeTask(
        TaskScheduleRequest request, WorkerContext context
    ) throws Exception;
}
