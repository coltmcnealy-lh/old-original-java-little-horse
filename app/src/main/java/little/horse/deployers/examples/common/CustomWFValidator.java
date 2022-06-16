package little.horse.deployers.examples.common;

import little.horse.common.LHConfig;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;

public interface CustomWFValidator {
    public void validate(WFSpec spec, LHConfig config) throws LHValidationError;
}
