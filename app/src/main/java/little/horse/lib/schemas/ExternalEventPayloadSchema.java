package little.horse.lib.schemas;

import java.util.Date;

public class ExternalEventPayloadSchema extends BaseSchema {
    public String externalEventGuid;
    public String externalEventDefGuid;
    public String externalEventDefName;
    public Object content;
    public Date timestamp;
}
