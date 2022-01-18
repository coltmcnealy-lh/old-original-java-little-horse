package little.horse.lib.objects;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.Config;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.schemas.ExternalEventDefSchema;

public class ExternalEventDef {
    private Config config;
    private ExternalEventDefSchema schema;

    public ExternalEventDef(ExternalEventDefSchema schema, Config config) throws LHValidationError {
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.name == null) {
            throw new LHValidationError(
                "No name provided on the task definition schema."
            );
        }

        // TODO: Support event types other than callbacks.

        this.schema = schema;
        this.config = config;
    }

    public ExternalEventDefSchema getModel() {
        return this.schema;
    }

    public String toString() {
        return schema.toString();
    }

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getExternalEventDefTopic(),
            schema.guid,
            this.toString()
        );
        this.config.send(record);
    }
}
