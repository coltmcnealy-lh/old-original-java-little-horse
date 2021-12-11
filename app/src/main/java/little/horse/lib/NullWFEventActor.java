package little.horse.lib;

public class NullWFEventActor implements WFEventProcessorActor {
    public void act(WFRunSchema wfRun, WFEventSchema event) {
        System.out.println(event.toString());
        System.out.println(wfRun.toString());
    }
}
