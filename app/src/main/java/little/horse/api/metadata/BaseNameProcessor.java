package little.horse.api.metadata;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.objects.metadata.CoreMetadata;

public class BaseNameProcessor<T extends CoreMetadata>
implements Processor<String, T, String, T> {
    private KeyValueStore<String, T> kvStore;

    @Override
    public void init(final ProcessorContext<String, T> context) {
        this.kvStore = context.getStateStore(T.getNameStoreName());
    }

    @Override
    public void process(final Record<String, T> record) {
        T t = record.value();
        if (t == null) {
            kvStore.delete(record.key());
        } else {
            kvStore.put(record.key(), t);
        }
    }
}
