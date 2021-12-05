package little.horse.lib.TaskDef;

import little.horse.lib.Config;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

public class TaskDef {
    private Config config;
    private TaskDefSchema schema;

    public TaskDef(TaskDefSchema schema, Config config) throws LHValidationError {
        // TODO (hard): do some validation that we don't have a 409.
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.bashCommand == null) {
            throw new LHValidationError(
                "No bash command provided on the task definition schema."
            );
        }

        if (schema.name == null) {
            throw new LHValidationError(
                "No name provided on the task definition schema."
            );
        }

        if (schema.dockerImage == null) {
            schema.dockerImage = config.getDefaultTaskDockerImage();
        }

        this.schema = schema;
        this.config = config;
    }

    public TaskDefSchema getModel() {
        return this.schema;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String result;
        try {
            result = mapper.writeValueAsString(this.getModel());
        } catch(JsonProcessingException exn) {
            System.out.println(exn.toString());
            result = "Could not serialize.";
        }
        return result;
    }

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getTaskDefTopic(),
            schema.guid,
            this.toString()
        );
        this.config.send(record);
    }
}
