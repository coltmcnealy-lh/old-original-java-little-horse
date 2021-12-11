package little.horse.lib;

public interface WFEventProcessorActor {
    public void act(WFRunSchema wfRun, WFEventSchema event);
}
