package little.horse.lib.schemas;


import java.util.ArrayList;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.Config;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;

public class TaskDefSchema extends BaseSchema {
    public String name;
    public String guid;
    public String dockerImage;
    public ArrayList<String> bashCommand;
    public String stdin;

    public void validateAndCleanup(Config config) throws LHValidationError {
        if (bashCommand == null) {
            throw new LHValidationError("Must provide a bash command!");
        }
        if (guid == null) guid = LHUtil.generateGuid();

        if (bashCommand == null) {
            throw new LHValidationError(
                "No bash command provided on the task definition schema."
            );
        }

        if (name == null) {
            throw new LHValidationError(
                "No name provided on the task definition schema."
            );
        }

        if (dockerImage == null) {
            dockerImage = config.getDefaultTaskDockerImage();
        }

        setConfig(config);
    }

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getTaskDefTopic(), guid, this.toString());
        this.config.send(record);
    }
}
