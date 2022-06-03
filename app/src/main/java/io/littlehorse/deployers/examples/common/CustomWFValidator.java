package io.littlehorse.deployers.examples.common;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.WFSpec;

public interface CustomWFValidator {
    public void validate(WFSpec spec, DepInjContext config) throws LHValidationError;
}
