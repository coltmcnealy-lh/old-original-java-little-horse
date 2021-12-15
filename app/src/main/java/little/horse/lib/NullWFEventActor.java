package little.horse.lib;

import little.horse.lib.schemas.WFEventSchema;

public class NullWFEventActor implements WFEventProcessorActor {
    public void act(WFRunSchema wfRun, WFEventSchema event) {
        System.out.println(event.toString());
        System.out.println(wfRun.toString());
    }
}
