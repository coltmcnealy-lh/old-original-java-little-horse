package little.horse.deployers.examples.common;

import little.horse.common.LHConfig;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface CustomTaskValidator {
    public void validate(TaskDef spec, LHConfig config) throws LHValidationError;
}
