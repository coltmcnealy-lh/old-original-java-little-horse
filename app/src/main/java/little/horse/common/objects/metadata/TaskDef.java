package little.horse.common.objects.metadata;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.util.LHDatabaseClient;


public class TaskDef extends CoreMetadata {
    @JsonIgnore
    public static String typeName = "taskDef";

    public TaskDef() {}

    public TaskDef(TaskQueue tq) {
        this.setTaskQueue(tq);
    }

    public HashMap<String, WFRunVariableDef> requiredVars;
    public String taskQueueName;

    @JsonIgnore
    private TaskQueue taskQueue;

    @JsonIgnore
    public TaskQueue getTaskQueue() throws LHConnectionError {
        if (taskQueue == null) {
            taskQueue = LHDatabaseClient.lookupMetaNameOrId(
                taskQueueName, config, TaskQueue.class
            );
        }
        return taskQueue;
    }

    @JsonIgnore
    public void setTaskQueue(TaskQueue tq) {
        this.taskQueue = tq;
    }

    public void processChange(CoreMetadata old) {
        if (old != null && !(old instanceof TaskDef)) {
            throw new RuntimeException(
                "Whatever code made this call is nincompoop."
            );
        }

        // Nothing really to do here since we TaskDef doesn't have side effects.
    }

    public void validate(Config config) throws LHValidationError, LHConnectionError {
        this.config = config;

        // ALl we gotta do is make sure the taskQueue exists.
        taskQueue = LHDatabaseClient.lookupMetaNameOrId(
            taskQueueName, config, TaskQueue.class
        );
        if (taskQueue == null) {
            throw new LHValidationError(String.format(
                "Task Def %s refers to nonexistent task queue %s!",
                name, taskQueueName
            ));
        }

    }
}
