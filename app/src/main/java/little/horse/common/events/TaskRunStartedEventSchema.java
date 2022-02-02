package little.horse.common.events;

import java.util.ArrayList;

import little.horse.common.objects.BaseSchema;

public class TaskRunStartedEventSchema extends BaseSchema {
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;
    public String nodeGuid;
    public String nodeName;
    public int taskRunNumber;
    public int threadID;
}
