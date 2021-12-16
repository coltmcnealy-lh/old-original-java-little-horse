package little.horse.lib.schemas;

import little.horse.lib.LHComparisonEnum;

public class EdgeConditionSchema extends BaseSchema {
    public VariableDefinitionSchema leftSide;
    public VariableDefinitionSchema rightSide;
    public LHComparisonEnum comparator;
}
