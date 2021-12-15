package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

public class WFRunVariableContexSchema extends BaseSchema {
    public HashMap<String, Object> inputVariables;
    public HashMap<String, Object> variables;
    public HashMap<String, ArrayList<TaskRunSchema>> taskRuns;
}
