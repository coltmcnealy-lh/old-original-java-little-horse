package little.horse.lib.deployers.examples.nulldeployer;

import little.horse.common.DepInjContext;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.lib.deployers.TaskDeployer;

public class NullTaskDeployer implements TaskDeployer {
    public void validate(TaskDef spec, DepInjContext config) {}

    public void deploy(TaskDef spec, DepInjContext config) {}

    public void undeploy(TaskDef spec, DepInjContext config) {}
    
}
