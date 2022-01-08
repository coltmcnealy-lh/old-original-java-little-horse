package little.horse.lib.schemas;

import java.util.ArrayList;

import little.horse.lib.LHFailureReason;
import little.horse.lib.wfRuntime.WFRunStatus;

public class WFTokenSchema extends BaseSchema {
    public ArrayList<TaskRunSchema> unscheduledNodes;
    public ArrayList<TaskRunSchema> taskRuns;
    public WFRunStatus status;
    public LHFailureReason errorCode;
    public String errorMessage;

    public Integer tokenNumber;

    public TaskRunSchema activeNode;
    public ArrayList<Integer> parentTokenNumbers;
    public ArrayList<Integer> childrenTokenNumbers;
}
