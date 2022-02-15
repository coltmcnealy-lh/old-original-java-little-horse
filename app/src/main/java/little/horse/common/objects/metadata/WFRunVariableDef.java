package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.objects.BaseSchema;
import little.horse.common.util.json.JsonMapKey;


public class WFRunVariableDef extends BaseSchema {
    public WFRunVariableTypeEnum type;
    public Object defaultValue;
    public boolean includedInResult = true;

    @JsonMapKey
    public String variableName;

    // @JsonBackReference("vardef-to-thread")
    @JsonIgnore
    public Thread thread;

    // @JsonBackReference("vardef-to-taskdef")
    @JsonIgnore
    public TaskDef taskDef;
}
