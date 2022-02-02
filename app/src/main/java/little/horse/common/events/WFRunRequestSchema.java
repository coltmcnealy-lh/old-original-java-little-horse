package little.horse.common.events;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpecSchema;

public class WFRunRequestSchema extends BaseSchema {
    public HashMap<String, Object> variables;
    public WFSpecSchema wfSpec;
}
