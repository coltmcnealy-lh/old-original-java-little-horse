package little.horse.lib.schemas;

import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.Config;

public class TaskScheduledEventSchema extends BaseSchema {
    public String wfRunGuid;
    public String wfSpecGuid;
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
