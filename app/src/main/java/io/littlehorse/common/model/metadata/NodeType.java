package io.littlehorse.common.model.metadata;

public enum NodeType {
    ENTRYPOINT,
    EXIT,
    TASK,
    EXTERNAL_EVENT,
    SPAWN_THREAD,
    WAIT_FOR_THREAD,
    SLEEP,
    NOP,
    THROW_EXCEPTION;
}
