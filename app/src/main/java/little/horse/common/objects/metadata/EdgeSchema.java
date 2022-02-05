package little.horse.common.objects.metadata;

import little.horse.common.objects.BaseSchema;

public class EdgeSchema extends BaseSchema {
    public String sourceNodeName;
    public String sinkNodeName;
    public EdgeConditionSchema condition;
}
