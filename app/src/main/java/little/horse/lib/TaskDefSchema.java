package little.horse.lib;

import java.util.ArrayList;

public class TaskDefSchema {
    public String name;
    public String guid;
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;
}
