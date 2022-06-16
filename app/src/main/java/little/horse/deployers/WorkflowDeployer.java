package little.horse.deployers;

import little.horse.common.LHConfig;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;

public interface WorkflowDeployer {
    public void validate(WFSpec spec, LHConfig config) throws LHValidationError;

    public void deploy(WFSpec spec, LHConfig config) throws LHConnectionError;

    public void undeploy(WFSpec spec, LHConfig config) throws LHConnectionError;
}
