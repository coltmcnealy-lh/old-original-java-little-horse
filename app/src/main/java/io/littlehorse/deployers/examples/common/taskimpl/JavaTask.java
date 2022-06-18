package io.littlehorse.deployers.examples.common.taskimpl;

import io.littlehorse.common.LHConfig;
import io.littlehorse.deployers.examples.common.DeployerConfig;
import io.littlehorse.scheduler.TaskScheduleRequest;

public interface JavaTask {
    public void init(DeployerConfig ddConfig, LHConfig config);

    public Object executeTask(
        TaskScheduleRequest request, WorkerContext context
    ) throws Exception;
}
