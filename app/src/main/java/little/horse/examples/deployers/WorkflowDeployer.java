package little.horse.examples.deployers;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;

public interface WorkflowDeployer {
    public void validate(WFSpec spec, Config config) throws LHValidationError;

    public void deploy(WFSpec spec, Config config) throws LHConnectionError;

    public void undeploy(WFSpec spec, Config config) throws LHConnectionError;
}
