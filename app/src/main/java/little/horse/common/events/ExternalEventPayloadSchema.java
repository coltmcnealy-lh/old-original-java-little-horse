package little.horse.common.events;

import java.util.Date;

import little.horse.common.objects.BaseSchema;

public class ExternalEventPayloadSchema extends BaseSchema {
    public String externalEventGuid;
    public String externalEventDefGuid;
    public String externalEventDefName;
    public Object content;
    public Date timestamp;
}
