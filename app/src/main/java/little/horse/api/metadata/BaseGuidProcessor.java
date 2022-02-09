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
        T newMeta = record.value();
        T old = kvStore.get(record.key());

        if (newMeta == null) {
            if (old != null) {
                old.remove();
            }
            kvStore.delete(record.key());
            return;
        }

        // It is somewhat frowned upon to do this within Kafka Streams. However,
        // the processChange is an idempotent, level-triggered method; so repeated
        // calls are not destructive. Furthermore, the deployment of TaskQueue's
        // needn't be super-low latency from when the original API call comes in,
        // because a) creating TaskQueue/deploying WFSpec goes through slow Kafka and
        // Kubernetes API's, and b) the expected throughput of Metadata changes is
        // low, so we don't have to be super fast.
        newMeta.processChange(old);

        kvStore.put(record.key(), newMeta);
        // Now, we re-key it.
        Record<String, T> nameKeyedRecord = new Record<>(
            newMeta.name,
            newMeta,
            record.timestamp()
        );
        context.forward(nameKeyedRecord);
    }
}