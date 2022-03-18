package little.horse.common.events;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class WFRunRequest extends BaseSchema {
    public HashMap<String, Object> variables;
    public String wfSpecId;
    public String wfRunId;
}
