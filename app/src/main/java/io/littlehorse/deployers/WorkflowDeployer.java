package io.littlehorse.deployers;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.WFSpec;

public interface WorkflowDeployer {
    public void validate(WFSpec spec, LHConfig config) throws LHValidationError;

    public void deploy(WFSpec spec, LHConfig config) throws LHConnectionError;

    public void undeploy(WFSpec spec, LHConfig config) throws LHConnectionError;
}
