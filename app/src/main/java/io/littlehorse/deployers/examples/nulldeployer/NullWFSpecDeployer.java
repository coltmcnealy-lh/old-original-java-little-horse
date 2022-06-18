package io.littlehorse.deployers.examples.nulldeployer;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.deployers.WorkflowDeployer;

public class NullWFSpecDeployer implements WorkflowDeployer {
    public void validate(WFSpec spec, LHConfig conf) {}

    public void deploy(WFSpec spec, LHConfig config) {}

    public void undeploy(WFSpec spec, LHConfig config) {}

}
