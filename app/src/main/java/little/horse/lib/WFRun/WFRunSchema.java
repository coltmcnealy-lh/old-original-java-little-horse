package little.horse.lib.WFRun;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.LHStatus;

public class WFRunSchema {
    public String guid;
    public String wfSpecGuid;
    public String wfSpecName;
    public ArrayList<TaskRunSchema> taskRuns;
    public LHStatus status;
    public HashMap<String, Object> variables;
    public HashMap<String, Object> inputVariables;
}
