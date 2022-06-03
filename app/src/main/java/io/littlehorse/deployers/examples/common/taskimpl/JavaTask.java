package io.littlehorse.deployers.examples.common.taskimpl;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.deployers.examples.common.DeployerConfig;
import io.littlehorse.scheduler.TaskScheduleRequest;

public interface JavaTask {
    public void init(DeployerConfig ddConfig, DepInjContext config);

    public Object executeTask(
        TaskScheduleRequest request, WorkerContext context
    ) throws Exception;
}
