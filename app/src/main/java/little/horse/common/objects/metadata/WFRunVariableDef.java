package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;

import little.horse.common.objects.BaseSchema;
import little.horse.common.util.json.JsonMapKey;


public class WFRunVariableDef extends BaseSchema {
    public WFRunVariableTypeEnum type;
    public Object defaultValue;
    public boolean includedInResult = true;

    @JsonMapKey
    public String variableName;

    @JsonBackReference
    public TaskDef thread;
}
