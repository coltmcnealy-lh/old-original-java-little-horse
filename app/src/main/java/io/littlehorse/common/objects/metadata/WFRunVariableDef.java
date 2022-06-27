package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.littlehorse.common.objects.BaseSchema;


public class WFRunVariableDef extends BaseSchema {
    public String id;

    public LHVarType type;
    public VariableValue defaultValue;
    public String defaultValueId;
    public String variableName;

    @JsonIgnore
    public Thread thread;
    public String threadSpecId;
}
