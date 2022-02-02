package little.horse.api.metadata;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.objects.metadata.CoreMetadata;

public class BaseGuidProcessor<T extends CoreMetadata>
implements Processor<String, T, String, T> {
    private KeyValueStore<String, T> kvStore;
    private ProcessorContext<String, T> context;

    @Override
    public void init(final ProcessorContext<String, T> context) {
        this.kvStore = context.getStateStore(T.getStoreName());
        this.context = context;
    }

    @Override
    public void process(final Record<String, T> record) {
        T t = record.value();
        if (t == null) {
            kvStore.delete(record.key());
            return;
        }

        T old = kvStore.get(record.key());
        Thread thread = new Thread(
            // This is where the actual deployment happens.
            () -> {t.processChange(old);}
        );
        thread.start();

        kvStore.put(record.key(), t);
        // Now, we re-key it.
        Record<String, T> nameKeyedRecord = new Record<>(
            t.name,
            t,
            record.timestamp()
        );
        context.forward(nameKeyedRecord);
    }
}