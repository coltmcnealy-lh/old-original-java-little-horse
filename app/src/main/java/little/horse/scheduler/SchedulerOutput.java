package little.horse.scheduler;

import little.horse.common.objects.rundata.WFRun;

/**
 * Well, this is pretty jank, but exactly one of the fields will be null and the other
 * will be populated.
 */
public class SchedulerOutput {
    public TaskScheduleRequest request;
    public WFRun wfRun;
}
