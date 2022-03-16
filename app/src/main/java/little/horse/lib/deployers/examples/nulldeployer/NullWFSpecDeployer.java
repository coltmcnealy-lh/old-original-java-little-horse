package little.horse.lib.deployers.examples.nulldeployer;

import little.horse.common.Config;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.lib.deployers.WorkflowDeployer;

public class NullWFSpecDeployer implements WorkflowDeployer {
    public void validate(WFSpec spec, Config conf) {}

    public void deploy(WFSpec spec, Config config) {}

    public void undeploy(WFSpec spec, Config config) {}

}
