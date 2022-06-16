package little.horse.deployers.examples.nulldeployer;

import little.horse.common.LHConfig;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.deployers.WorkflowDeployer;

public class NullWFSpecDeployer implements WorkflowDeployer {
    public void validate(WFSpec spec, LHConfig conf) {}

    public void deploy(WFSpec spec, LHConfig config) {}

    public void undeploy(WFSpec spec, LHConfig config) {}

}
