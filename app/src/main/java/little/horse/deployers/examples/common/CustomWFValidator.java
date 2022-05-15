package little.horse.deployers.examples.common;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;

public interface CustomWFValidator {
    public void validate(WFSpec spec, DepInjContext config) throws LHValidationError;
}
