package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.objects.BaseSchema;
import little.horse.common.util.json.JsonMapKey;


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
