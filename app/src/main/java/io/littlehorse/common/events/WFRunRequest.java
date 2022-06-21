package io.littlehorse.common.events;

import java.util.HashMap;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.VariableValue;

public class WFRunRequest extends BaseSchema {
    public HashMap<String, VariableValue> variables;
    public String wfSpecId;
    public String wfRunId;
}
