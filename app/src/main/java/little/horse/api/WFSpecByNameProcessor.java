package little.horse.api;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.util.Constants;

public class WFSpecByNameProcessor implements Processor<
String, WFSpecSchema, String, WFSpecSchema
>{
    private KeyValueStore<String, WFSpecSchema> kvStore;
    private ProcessorContext<String, WFSpecSchema> context;

    @Override
    public void init(final ProcessorContext<String, WFSpecSchema> context) {
        this.kvStore = context.getStateStore(Constants.WF_SPEC_NAME_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, WFSpecSchema> record) {
        WFSpecSchema wfs = record.value();
        if (wfs == null) {
            kvStore.delete(record.key());
        } else {
            kvStore.put(record.key(), wfs);
        }
        context.forward(record);
    }
}
