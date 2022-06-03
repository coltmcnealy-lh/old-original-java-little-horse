package io.littlehorse.deployers;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.TaskDef;

public interface TaskDeployer {
    public void validate(TaskDef spec, DepInjContext config)
        throws LHValidationError, LHConnectionError;

    public void deploy(TaskDef spec, DepInjContext config) throws LHConnectionError;

    public void undeploy(TaskDef spec, DepInjContext config) throws LHConnectionError;
    
}
