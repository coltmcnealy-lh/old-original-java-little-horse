package io.littlehorse.common.events;

import java.util.HashMap;

import io.littlehorse.common.objects.BaseSchema;

public class WFRunRequest extends BaseSchema {
    public HashMap<String, Object> variables;
    public String wfSpecId;
    public String wfRunId;
}
