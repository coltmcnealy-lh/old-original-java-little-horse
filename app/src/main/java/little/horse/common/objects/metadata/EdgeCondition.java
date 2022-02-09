package little.horse.common.objects.metadata;

import little.horse.common.objects.BaseSchema;

public class EdgeCondition extends BaseSchema {
    public VariableAssignment leftSide;
    public VariableAssignment rightSide;
    public LHComparisonEnum comparator;
}
