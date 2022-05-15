package little.horse.deployers.examples.nulldeployer;

import little.horse.common.DepInjContext;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.deployers.WorkflowDeployer;

public class NullWFSpecDeployer implements WorkflowDeployer {
    public void validate(WFSpec spec, DepInjContext conf) {}

    public void deploy(WFSpec spec, DepInjContext config) {}

    public void undeploy(WFSpec spec, DepInjContext config) {}

}
