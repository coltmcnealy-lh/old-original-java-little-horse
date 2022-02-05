package little.horse.common.objects.metadata;


import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.common.Config;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;

public class TaskDefSchema extends CoreMetadata {
    public HashMap<String, WFRunVariableDefSchema> requiredVars;
    public String taskQueueName;
    public String taskType;

    private String guid;

    public String getGuid() {
        if (guid != null) return guid;

        StringBuilder content = new StringBuilder();
        content.append(requiredVars.toString());


        guid = content.toString();
        return guid;
    }

    public void processChange(CoreMetadata old) {
        if (!(old instanceof TaskDefSchema)) {
            throw new RuntimeException("Tried to process change on bad type!");
        }

        // TaskDefSchema oldTd = (TaskDefSchema) old;

        throw new RuntimeException("Implement me!");
    }

    public void validateAndCleanup(Config config) throws LHValidationError {
        if (taskQueueName == null) {
            throw new LHValidationError(
                "Must provide task queue name!"
            );
        }

        if (taskType == null) {
            throw new LHValidationError(
                "No bash command provided on the task definition schema."
            );
        }

        if (requiredVars == null) {
            requiredVars = new HashMap<>();
        }

        setConfig(config);
    }

    public void record() {
        // TODO: This method should be deprecated or something now that we don't
        // have static workflow definitions.
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getTaskDefTopic(), LHUtil.generateGuid(), this.toString());
        this.config.send(record);
    }
}
