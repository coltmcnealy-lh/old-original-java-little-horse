package little.horse.lib.schemas;

import little.horse.lib.LHComparisonEnum;

public class EdgeConditionSchema extends BaseSchema {
    public VariableAssignmentSchema leftSide;
    public VariableAssignmentSchema rightSide;
    public LHComparisonEnum comparator;
}
