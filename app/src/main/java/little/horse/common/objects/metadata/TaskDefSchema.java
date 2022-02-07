package little.horse.common.objects.metadata;

import java.util.HashMap;

public class TaskDefSchema extends CoreMetadata {
    public HashMap<String, WFRunVariableDefSchema> requiredVars;
    public String taskQueueName;
    public String taskType;

    public void processChange(CoreMetadata old) {
        if (!(old instanceof TaskDefSchema)) {
            throw new RuntimeException(
                "Whatever code made this call is nincompoop."
            );
        }

        // TaskDefSchema oldTd = (TaskDefSchema) old;
        throw new RuntimeException("Implement me!");
    }
}
