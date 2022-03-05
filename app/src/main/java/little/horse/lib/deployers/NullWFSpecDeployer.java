package little.horse.lib.deployers;

import little.horse.common.Config;
import little.horse.common.objects.metadata.WFSpec;

public class NullWFSpecDeployer implements WorkflowDeployer {
    public void validate(WFSpec spec, Config conf) {}

    public void deploy(WFSpec spec, Config config) {}

    public void undeploy(WFSpec spec, Config config) {}

}
