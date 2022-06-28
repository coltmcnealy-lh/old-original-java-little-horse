package io.littlehorse.common.model.metadata;

import io.littlehorse.common.model.BaseSchema;

public class EdgeCondition extends BaseSchema {
    public VariableAssignment leftSide;
    public VariableAssignment rightSide;
    public LHComparisonEnum comparator;
}
