package little.horse.common.events;

import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;

public class TaskScheduledEvent extends BaseSchema {
    public String wfRunGuid;
    public String wfSpecDigest;
    public String wfSpecName;
    public String taskType;
    public String taskQueueName;

    public String taskExecutionGuid;

    public HashMap<String, Object> variables;

    public void record(Config config) {
        setConfig(config);
        record();
    }

    public void record() {
        if (config == null) {
            throw new RuntimeException("Null config!");
        }
        if (taskQueueName == null || taskType == null) {
            throw new RuntimeException("You moron");
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
            taskExecutionGuid,
            wfRunGuid,
            this.toString()
        );
        config.send(record);
    }
}