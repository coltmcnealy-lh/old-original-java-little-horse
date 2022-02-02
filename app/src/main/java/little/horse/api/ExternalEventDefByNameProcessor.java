package little.horse.api;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.objects.metadata.ExternalEventDefSchema;
import little.horse.common.util.Constants;


public class ExternalEventDefByNameProcessor implements Processor<
String, ExternalEventDefSchema, String, ExternalEventDefSchema
>  {
    private KeyValueStore<String, ExternalEventDefSchema> kvStore;
    private ProcessorContext<String, ExternalEventDefSchema> context;

    @Override
    public void init(final ProcessorContext<String, ExternalEventDefSchema> context) {
        kvStore = context.getStateStore(Constants.EXTERNAL_EVENT_DEF_NAME_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, ExternalEventDefSchema> record) {
        ExternalEventDefSchema td = record.value();
        if (td == null) {
            kvStore.delete(record.key());
        } else {
            kvStore.put(record.key(), td);
        }

        context.forward(record);
    }
}
