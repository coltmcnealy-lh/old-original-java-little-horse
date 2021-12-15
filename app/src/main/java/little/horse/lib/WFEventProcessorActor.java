package little.horse.lib;

import little.horse.lib.schemas.WFEventSchema;

public interface WFEventProcessorActor {
    public void act(WFRunSchema wfRun, WFEventSchema event);
}
