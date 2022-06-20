package io.littlehorse.common.objects.rundata;

import io.littlehorse.common.objects.metadata.VariableValue;
import io.littlehorse.common.objects.metadata.WFRunVariableDef;

public class VariableLookupResult {
    public WFRunVariableDef varDef;
    public ThreadRun thread;
    public VariableValue value;

    public VariableLookupResult(
        WFRunVariableDef varDef, ThreadRun thread, VariableValue value
    ) {
        this.value = value;
        this.varDef = varDef;
        this.thread = thread;
    }
}
