package little.horse.deployers;

import little.horse.common.LHConfig;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface TaskDeployer {
    public void validate(TaskDef spec, LHConfig config)
        throws LHValidationError, LHConnectionError;

    public void deploy(TaskDef spec, LHConfig config) throws LHConnectionError;

    public void undeploy(TaskDef spec, LHConfig config) throws LHConnectionError;
    
}
