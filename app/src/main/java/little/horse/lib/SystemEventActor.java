package little.horse.lib;

import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;

public class SystemEventActor implements WFEventProcessorActor {
    public String getNodeGuid() {
        return null;
    }

    public void act(WFRunSchema wfRun, WFEventSchema event, int taskRunNumber) {
    }
}
