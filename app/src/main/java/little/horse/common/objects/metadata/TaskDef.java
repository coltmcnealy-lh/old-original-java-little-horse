package little.horse.common.objects.metadata;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.util.LHDatabaseClient;


public class TaskDef extends CoreMetadata {
    public static String typeName = "taskDef";

    public TaskDef() {}

    public TaskDef(TaskQueue tq) {
        this.setTaskQueue(tq);
    }

    public HashMap<String, WFRunVariableDef> requiredVars;
    public String taskQueueName;
    public String taskType;

    @JsonIgnore
    private TaskQueue taskQueue;

    public TaskQueue getTaskQueue() throws LHConnectionError {
        if (taskQueue == null) {
            taskQueue = LHDatabaseClient.lookupMeta(
                taskQueueName, config, TaskQueue.class
            );
        }
        return taskQueue;
    }

    public void setTaskQueue(TaskQueue tq) {
        this.taskQueue = tq;
    }

    public void processChange(CoreMetadata old) {
        if (!(old instanceof TaskDef)) {
            throw new RuntimeException(
                "Whatever code made this call is nincompoop."
            );
        }

        // Nothing really to do here since we TaskDef doesn't have side effects.
    }
}
