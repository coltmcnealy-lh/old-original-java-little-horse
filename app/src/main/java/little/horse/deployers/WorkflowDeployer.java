package little.horse.deployers;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;

public interface WorkflowDeployer {
    public void validate(WFSpec spec, DepInjContext config) throws LHValidationError;

    public void deploy(WFSpec spec, DepInjContext config) throws LHConnectionError;

    public void undeploy(WFSpec spec, DepInjContext config) throws LHConnectionError;
}
