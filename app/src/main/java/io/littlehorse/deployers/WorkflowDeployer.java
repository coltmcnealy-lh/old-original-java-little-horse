package io.littlehorse.deployers;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.WFSpec;

public interface WorkflowDeployer {
    public void validate(WFSpec spec, DepInjContext config) throws LHValidationError;

    public void deploy(WFSpec spec, DepInjContext config) throws LHConnectionError;

    public void undeploy(WFSpec spec, DepInjContext config) throws LHConnectionError;
}
