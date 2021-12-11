package little.horse.lib;

import java.util.ArrayList;

public class TaskRunStartedEventSchema {
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;
    public String nodeIdentifier;
}
