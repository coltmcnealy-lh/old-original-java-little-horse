package little.horse.lib.objects;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHLookupExceptionReason;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.TaskDefSchema;

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
        return schema.toString();
    }

    public ArrayList<String> getTaskDaemonCommand() {
        return config.getTaskDaemonCommand();
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
