package io.littlehorse.scheduler;

import io.littlehorse.proto.TaskScheduleRequestPb;
import io.littlehorse.proto.WFRunPb;

public class SchedulerOutput {
    public WFRunPb.Builder newRun;
    public TaskScheduleRequestPb.Builder toSchedule;

    public SchedulerOutput(WFRunPb.Builder newRun) {
        this.newRun = newRun;
    }

    public SchedulerOutput(TaskScheduleRequestPb.Builder ts) {
        this.toSchedule = ts;
    }

    public SchedulerOutput() {}
}
