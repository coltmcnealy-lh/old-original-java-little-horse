package little.horse.lib.schemas;

import little.horse.lib.WFEventType;

public class WFTriggerSchema extends BaseSchema {
    public String triggerNodeName;
    public String triggerNodeGuid;
    public WFEventType triggerEventType;
}