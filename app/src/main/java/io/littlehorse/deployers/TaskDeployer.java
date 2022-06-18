package io.littlehorse.deployers;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.TaskDef;

public interface TaskDeployer {
    public void validate(TaskDef spec, LHConfig config)
        throws LHValidationError, LHConnectionError;

    public void deploy(TaskDef spec, LHConfig config) throws LHConnectionError;

    public void undeploy(TaskDef spec, LHConfig config) throws LHConnectionError;
    
}
