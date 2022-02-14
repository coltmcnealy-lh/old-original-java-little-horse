package little.horse.api.runtime;

import little.horse.common.objects.rundata.WFRun;

/**
 * Well, this is pretty jank, but exactly one of the fields will be null and the other
 * will be populated.
 */
public class CoordinatorOutput {
    public TaskScheduleRequest request;
    public WFRun wfRun;
}
