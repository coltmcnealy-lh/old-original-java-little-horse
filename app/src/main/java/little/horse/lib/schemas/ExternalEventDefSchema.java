package little.horse.lib.schemas;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.LHUtil;

public class ExternalEventDefSchema extends BaseSchema {
    public String name;
    public String guid;

    public void validateAndCleanup() {
        if (guid == null) guid = LHUtil.generateGuid();
    }
    // TODO: Maybe make it possible to trigger one in ways other than a callback.

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getExternalEventDefTopic(), guid, this.toString());
        this.config.send(record);
    }
}
