package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.LHFailureReason;
import little.horse.lib.LHStatus;

public class WFRunSchema extends BaseSchema {
    public String guid;
    public String wfSpecGuid;
    public String wfSpecName;
    public ArrayList<TaskRunSchema> taskRuns;
    public LHStatus status;
    public HashMap<String, Object> variables;
    public HashMap<String, Object> inputVariables;

    public LHFailureReason errorCode;
    public String errorMessage;

    public HashMap<String, ArrayList<ExternalEventThingySchema>> pendingEvents;
}
