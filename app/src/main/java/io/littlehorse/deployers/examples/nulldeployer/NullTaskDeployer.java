package io.littlehorse.deployers.examples.nulldeployer;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.metadata.TaskDef;
import io.littlehorse.deployers.TaskDeployer;

public class NullTaskDeployer implements TaskDeployer {
    public void validate(TaskDef spec, LHConfig config) {}

    public void deploy(TaskDef spec, LHConfig config) {}

    public void undeploy(TaskDef spec, LHConfig config) {}
    
}
