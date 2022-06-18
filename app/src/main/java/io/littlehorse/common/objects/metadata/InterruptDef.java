package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.json.JsonMapKey;

public class InterruptDef extends BaseSchema {
    public String handlerThreadName;

    @JsonBackReference
    public ThreadSpec parent;

    @JsonMapKey
    public String externalEventName;
}
