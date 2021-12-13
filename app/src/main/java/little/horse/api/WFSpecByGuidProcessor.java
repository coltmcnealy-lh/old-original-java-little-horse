package little.horse.api;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Constants;
import little.horse.lib.LHStatus;
import little.horse.lib.WFSpecSchema;

public class WFSpecByGuidProcessor implements Processor<
String, WFSpecSchema, String, WFSpecSchema
>{
    private KeyValueStore<String, WFSpecSchema> kvStore;
    private ProcessorContext<String, WFSpecSchema> context;

    @Override
    public void init(final ProcessorContext<String, WFSpecSchema> context) {
        this.kvStore = context.getStateStore(Constants.WF_SPEC_GUID_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, WFSpecSchema> record) {
        WFSpecSchema wfs = record.value();
        kvStore.put(record.key(), wfs);

        // Now, we re-key it.
        Record<String, WFSpecSchema> nameKeyedRecord = new Record<>(
            wfs.name,
            wfs,
            record.timestamp()
        );
        context.forward(nameKeyedRecord);
    }

}
