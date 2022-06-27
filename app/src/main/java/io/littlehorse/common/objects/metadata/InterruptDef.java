package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.littlehorse.common.objects.BaseSchema;

public class InterruptDef extends BaseSchema {
    public String handlerThreadName;

    @JsonBackReference
    public ThreadSpec parent;
    public String externalEventName;
}
