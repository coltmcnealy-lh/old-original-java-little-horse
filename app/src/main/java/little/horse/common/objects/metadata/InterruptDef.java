package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;

import little.horse.common.objects.BaseSchema;
import little.horse.common.util.json.JsonMapKey;

public class InterruptDef extends BaseSchema {
    public String handlerThreadName;

    @JsonBackReference
    public ThreadSpec parent;

    @JsonMapKey
    public String externalEventName;
}
