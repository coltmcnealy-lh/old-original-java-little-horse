package io.littlehorse.deployers.examples.nulldeployer;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.deployers.WorkflowDeployer;

public class NullWFSpecDeployer implements WorkflowDeployer {
    public void validate(WFSpec spec, DepInjContext conf) {}

    public void deploy(WFSpec spec, DepInjContext config) {}

    public void undeploy(WFSpec spec, DepInjContext config) {}

}
