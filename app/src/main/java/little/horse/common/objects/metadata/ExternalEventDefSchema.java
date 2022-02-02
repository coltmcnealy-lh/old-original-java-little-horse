package little.horse.common.objects.metadata;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;

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
