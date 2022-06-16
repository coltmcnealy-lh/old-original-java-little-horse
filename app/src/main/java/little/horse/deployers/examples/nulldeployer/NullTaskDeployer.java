package little.horse.deployers.examples.nulldeployer;

import little.horse.common.LHConfig;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.deployers.TaskDeployer;

public class NullTaskDeployer implements TaskDeployer {
    public void validate(TaskDef spec, LHConfig config) {}

    public void deploy(TaskDef spec, LHConfig config) {}

    public void undeploy(TaskDef spec, LHConfig config) {}
    
}
