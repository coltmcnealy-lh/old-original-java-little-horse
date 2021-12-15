package little.horse.lib.schemas;

import java.util.ArrayList;

public class TaskDefSchema extends BaseSchema {
    public String name;
    public String guid;
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;
}
