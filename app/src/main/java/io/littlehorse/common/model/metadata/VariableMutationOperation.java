package io.littlehorse.common.model.metadata;

public enum VariableMutationOperation {
    ASSIGN,
    ADD,
    EXTEND,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    REMOVE_IF_PRESENT,
    REMOVE_INDEX,
    REMOVE_KEY;
}
