package little.horse.lib.schemas;

import java.util.Date;

import little.horse.lib.WFEventType;

public class WFEventSchema extends BaseSchema {
    public String wfSpecGuid;
    public String wfSpecName;
    public String wfRunGuid;
    public Date timestamp;
    public int wfTokenNumber;

    public WFEventType type;

    // In the case of a Task Run, for example, this is just a serialized TaskRunSchema
    // object. For other things, such as external events, it'll be other things.
    public String content;
}
