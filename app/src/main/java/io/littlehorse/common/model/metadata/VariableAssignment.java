package io.littlehorse.common.model.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.littlehorse.common.model.BaseSchema;

public class VariableAssignment extends BaseSchema {
    // A Task-level variable can be assigned to one of three things:
    // 1. A WFRunVariable
    // 2. A literal value
    // 3. Something from the metadata of the ThreadRun or WFRun.
    public String wfRunVariableName;
    public VariableValue literalValue;

    // It can also be jsonpath'ed
    public String jsonPath;

    // And you can provide a default (:
    public VariableValue defaultValue;

    @JsonBackReference
    public Node node;
}
