package little.horse.lib.schemas;

import java.util.HashMap;

public class WFRunRequestSchema extends BaseSchema {
    public HashMap<String, Object> variables;
    public WFSpecSchema wfSpec;
}
