package little.horse.lib.TaskDef;

import java.util.ArrayList;

public class TaskDefSchema {
    public String name;
    public String guid;
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;
}
