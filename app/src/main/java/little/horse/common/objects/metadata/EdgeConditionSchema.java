package little.horse.common.objects.metadata;

import little.horse.common.objects.BaseSchema;

public class EdgeConditionSchema extends BaseSchema {
    public VariableAssignmentSchema leftSide;
    public VariableAssignmentSchema rightSide;
    public LHComparisonEnum comparator;
}
