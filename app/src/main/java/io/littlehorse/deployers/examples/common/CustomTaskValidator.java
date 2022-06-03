package io.littlehorse.deployers.examples.common;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.TaskDef;

public interface CustomTaskValidator {
    public void validate(TaskDef spec, DepInjContext config) throws LHValidationError;
}
