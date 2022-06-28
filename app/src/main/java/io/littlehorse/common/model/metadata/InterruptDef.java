package io.littlehorse.common.model.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.littlehorse.common.model.BaseSchema;

public class InterruptDef extends BaseSchema {
    public String handlerThreadName;

    @JsonBackReference
    public ThreadSpec parent;
    public String externalEventName;
}
