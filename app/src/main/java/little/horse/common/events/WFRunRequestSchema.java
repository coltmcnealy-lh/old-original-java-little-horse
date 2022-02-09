package little.horse.common.events;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpec;

public class WFRunRequestSchema extends BaseSchema {
    public HashMap<String, Object> variables;
    public WFSpec wfSpec;
}
