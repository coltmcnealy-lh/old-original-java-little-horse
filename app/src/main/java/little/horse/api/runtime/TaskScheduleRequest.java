package little.horse.api.runtime;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class TaskScheduleRequest extends BaseSchema {
    public String taskDefName;
    public String taskDefId;
    public String taskQueueName;
    public String taskType;

    public String wfRunId;
    public String wfSpecId;
    public String wfSpecName;
    public int threadRunNumber;
    public int taskRunNumber;

    public HashMap<String, Object> variableSubstitutions;

    /**
     * The kafka topic to which the TaskCompletedEvent should be fired.
     */
    public String kafkaTopic;
}
