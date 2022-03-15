package little.horse.examples.deployers;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface TaskDeployer {
    public void validate(TaskDef spec, Config config)
        throws LHValidationError, LHConnectionError;

    public void deploy(TaskDef spec, Config config) throws LHConnectionError;

    public void undeploy(TaskDef spec, Config config) throws LHConnectionError;
    
}
