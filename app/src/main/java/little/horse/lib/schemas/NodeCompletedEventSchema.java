package little.horse.lib.schemas;

import java.util.ArrayList;

public class NodeCompletedEventSchema extends BaseSchema {
    public int taskExecutionNumber;
    public String stdout;
    public String stderr;
    public int returncode;
    public String nodeGuid;
    public ArrayList<String> bashCommand;
}
