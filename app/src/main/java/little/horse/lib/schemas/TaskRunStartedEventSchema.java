package little.horse.lib.schemas;

import java.util.ArrayList;

public class TaskRunStartedEventSchema extends BaseSchema {
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;
    public String nodeGuid;
    public String nodeName;
    public int taskRunNumber;
    public int threadID;
}
