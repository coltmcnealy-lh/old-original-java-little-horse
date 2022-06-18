package io.littlehorse.deployers.examples.common;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.WFSpec;

public interface CustomWFValidator {
    public void validate(WFSpec spec, LHConfig config) throws LHValidationError;
}
