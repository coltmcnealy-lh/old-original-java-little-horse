package little.horse.api.metadata;

import java.util.Optional;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.LHUtil;


public class OffsetProcessor<T extends CoreMetadata>
implements Processor<String, T, String, T> {
    private KeyValueStore<Integer, Long> kvStore;
    private ProcessorContext<String, T> context;

    @Override
    public void init(final ProcessorContext<String, T> context) {
        this.kvStore = context.getStateStore(T.getOffsetStoreName());
        this.context = context;
    }

    @Override
    public void process(final Record<String, T> record) {
        Optional<RecordMetadata> metaOp = context.recordMetadata();

        if (!metaOp.isPresent()) {
            LHUtil.logError("Meta was null!");
            return;
        }

        RecordMetadata meta = metaOp.get();

        Integer partition = meta.partition();
        Long offset = meta.offset();
        kvStore.put(partition, offset);
    }
}
