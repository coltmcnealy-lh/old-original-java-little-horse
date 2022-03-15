package little.horse.examples.deployers;

import little.horse.common.Config;
import little.horse.common.objects.metadata.TaskDef;

public class NullTaskDeployer implements TaskDeployer {
    public void validate(TaskDef spec, Config config) {}

    public void deploy(TaskDef spec, Config config) {}

    public void undeploy(TaskDef spec, Config config) {}
    
}
