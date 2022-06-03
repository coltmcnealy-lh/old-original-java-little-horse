package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.json.JsonMapKey;


public class WFRunVariableDef extends BaseSchema {
    public WFRunVariableTypeEnum type;
    public Object defaultValue;

    @JsonMapKey
    public String variableName;

    @JsonIgnore
    public Thread thread;

    @JsonIgnore
    public TaskDef taskDef;
}
