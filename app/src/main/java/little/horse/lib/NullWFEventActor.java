package little.horse.lib;

import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;

public class NullWFEventActor implements WFEventProcessorActor {
    public void act(WFRunSchema wfRun, WFEventSchema event) {
    }
}
