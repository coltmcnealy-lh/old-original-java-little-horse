package little.horse.lib.deployers.examples.docker;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface DockerSecondaryTaskValidator {
    public void validate(TaskDef spec, DepInjContext config) throws LHValidationError;
}
