package little.horse.common.events;

import java.util.Date;

import little.horse.common.objects.BaseSchema;

public class ExternalEventPayload extends BaseSchema {
    public String externalEventDefId;
    public String externalEventDefName;
    public Object content;
    public Date timestamp;
}
