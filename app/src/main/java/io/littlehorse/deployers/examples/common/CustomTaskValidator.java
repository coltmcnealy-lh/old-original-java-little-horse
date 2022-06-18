package io.littlehorse.deployers.examples.common;

import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.metadata.TaskDef;

public interface CustomTaskValidator {
    public void validate(TaskDef spec, LHConfig config) throws LHValidationError;
}
