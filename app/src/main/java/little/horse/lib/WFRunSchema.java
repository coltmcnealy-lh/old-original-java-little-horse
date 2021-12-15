package little.horse.lib;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.schemas.TaskRunSchema;

public class WFRunSchema {
    public String guid;
    public String wfSpecGuid;
    public String wfSpecName;
    public ArrayList<TaskRunSchema> taskRuns;
    public LHStatus status;
    public HashMap<String, Object> variables;
    public HashMap<String, Object> inputVariables;
}
