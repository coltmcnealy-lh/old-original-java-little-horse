package little.horse.common.events;


import little.horse.common.objects.BaseSchema;

public class TaskRunStartedEvent extends BaseSchema {
    public String workerId;
    public String stdin;
    public String nodeName;
    public int taskRunNumber;
    public int threadRunNumber;
}
