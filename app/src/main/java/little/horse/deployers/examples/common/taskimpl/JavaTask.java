package little.horse.deployers.examples.common.taskimpl;

import little.horse.common.DepInjContext;
import little.horse.deployers.examples.common.DeployerConfig;
import little.horse.scheduler.TaskScheduleRequest;

public interface JavaTask {
    public void init(DeployerConfig ddConfig, DepInjContext config);

    public Object executeTask(
        TaskScheduleRequest request, WorkerContext context
    ) throws Exception;
}
