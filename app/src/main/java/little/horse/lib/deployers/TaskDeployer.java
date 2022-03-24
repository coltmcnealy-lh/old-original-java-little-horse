package little.horse.lib.deployers;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface TaskDeployer {
    public void validate(TaskDef spec, DepInjContext config)
        throws LHValidationError, LHConnectionError;

    public void deploy(TaskDef spec, DepInjContext config) throws LHConnectionError;

    public void undeploy(TaskDef spec, DepInjContext config) throws LHConnectionError;
    
}
