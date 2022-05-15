package little.horse.deployers.examples.common;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;

public interface CustomTaskValidator {
    public void validate(TaskDef spec, DepInjContext config) throws LHValidationError;
}
