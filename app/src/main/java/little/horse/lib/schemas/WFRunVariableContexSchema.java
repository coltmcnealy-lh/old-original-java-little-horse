package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

public class WFRunVariableContexSchema extends BaseSchema {
    public HashMap<String, Object> wfRunVariables;
    public HashMap<String, ArrayList<TaskRunSchema>> nodeOutputs;
}
