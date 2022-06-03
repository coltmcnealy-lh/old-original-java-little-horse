package io.littlehorse.common.objects.metadata;

public enum NodeType {
    TASK,
    EXTERNAL_EVENT,
    SPAWN_THREAD,
    WAIT_FOR_THREAD,
    SLEEP,
    NOP,
    THROW_EXCEPTION;
}
