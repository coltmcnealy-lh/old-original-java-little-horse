package io.littlehorse.common.objects.metadata;

import io.littlehorse.common.objects.BaseSchema;

public class EdgeCondition extends BaseSchema {
    public VariableAssignment leftSide;
    public VariableAssignment rightSide;
    public LHComparisonEnum comparator;
}
