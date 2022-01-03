package little.horse.lib;

import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;

public interface WFEventProcessorActor {
    public void act(WFRunSchema wfRun, WFEventSchema event, int taskRunNumber);
    public String getNodeGuid();
}
