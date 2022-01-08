package little.horse.lib;

import little.horse.lib.schemas.WFRunSchema;

public interface WFEventProcessorActor {
    public void act(WFRunSchema wfRun, int tokenNumber, int taskRunNumber);
    public String getNodeGuid();
}
