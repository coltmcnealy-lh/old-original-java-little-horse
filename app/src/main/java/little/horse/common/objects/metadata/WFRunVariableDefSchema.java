package little.horse.common.objects.metadata;

import little.horse.common.objects.BaseSchema;

public class WFRunVariableDefSchema extends BaseSchema {
    public WFRunVariableTypeEnum type;
    public Object defaultValue;
    public boolean includedInResult = true;
}
