package little.horse.workflowworker;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class TaskScheduleRequest extends BaseSchema {
    public String taskDefId;
    public String taskDefName;

    public String wfRunId;
    public String wfSpecId;
    public String wfSpecName;
    public int threadId;
    public int taskRunPosition;

    public HashMap<String, Object> variableSubstitutions;

    /**
     * The kafka topic to which the TaskCompletedEvent should be fired.
     */
    public String kafkaTopic;
}
