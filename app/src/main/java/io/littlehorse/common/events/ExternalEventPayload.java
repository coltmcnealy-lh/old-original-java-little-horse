package io.littlehorse.common.events;

import java.util.Date;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.VariableValue;

public class ExternalEventPayload extends BaseSchema {
    public String externalEventDefId;
    public String externalEventDefName;
    public VariableValue content;
    public Date timestamp;
}
