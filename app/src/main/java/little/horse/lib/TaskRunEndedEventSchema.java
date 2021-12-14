package little.horse.lib;

import java.util.ArrayList;

public class TaskRunEndedEventSchema {
    public String stdout;
    public String stderr;
    public int returncode;
    public String nodeGuid;
    public ArrayList<String> bashCommand;
}
