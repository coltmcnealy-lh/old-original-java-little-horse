package little.horse.examples.deployers.docker;

import little.horse.common.Config;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface DockerSecondaryTaskValidator {
    public void validate(TaskDef spec, Config config) throws LHValidationError;
}
