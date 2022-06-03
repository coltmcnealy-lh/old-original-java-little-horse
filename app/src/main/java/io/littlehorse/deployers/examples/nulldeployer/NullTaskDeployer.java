package io.littlehorse.deployers.examples.nulldeployer;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.objects.metadata.TaskDef;
import io.littlehorse.deployers.TaskDeployer;

public class NullTaskDeployer implements TaskDeployer {
    public void validate(TaskDef spec, DepInjContext config) {}

    public void deploy(TaskDef spec, DepInjContext config) {}

    public void undeploy(TaskDef spec, DepInjContext config) {}
    
}
