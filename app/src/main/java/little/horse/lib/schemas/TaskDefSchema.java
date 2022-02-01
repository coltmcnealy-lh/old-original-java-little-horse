package little.horse.lib.schemas;


import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.Config;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;

public class TaskDefSchema extends BaseSchema {
    public HashMap<String, WFRunVariableDefSchema> requiredVars;
    public String taskQueueName;
    public String taskType;

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
