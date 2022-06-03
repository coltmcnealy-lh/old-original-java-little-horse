package io.littlehorse.api.metadata;

import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.util.Optional;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.metadata.GETable;

public class ResourceIndexProcessor<T extends GETable>
implements Processor<String, IndexEvent, Void, Void> {
    private KeyValueStore<String, Bytes> kvStore;
    private ProcessorContext<Void, Void> context;
    private Class<T> cls;

    public ResourceIndexProcessor(Class<T> cls, DepInjContext config) {
        this.cls = cls;
        // this.config = config;
    }

    @Override
    public void init(final ProcessorContext<Void, Void> context) {
        this.kvStore = context.getStateStore(T.getIndexStoreName(cls));
        this.context = context;
    }

    @Override
    public void process(final Record<String, IndexEvent> record) {
        try {
            processHelper(record);
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            // In the future, maybe implement a deadletter queue.
        }
    }

    private void processHelper(final Record<String, IndexEvent> record)
    throws LHSerdeError {
        IndexEvent idxEvt = record.value();

        IndexRecordKey key = idxEvt.indexKey;
        String storeKey = key.toString();  // Is the same as record.key()

        Optional<RecordMetadata> rm = context.recordMetadata();
        RecordMetadata recordMeta = rm.isPresent() ? rm.get() : null;
        Long offset = recordMeta == null ? null : recordMeta.offset();
        Integer partition = recordMeta == null ? null : recordMeta.partition();

        
        if (idxEvt.operation == IndexOperation.DELETE) {
            kvStore.delete(storeKey);
            
        } else if (idxEvt.operation == IndexOperation.CREATE) {
            IndexEntry entry = new IndexEntry();
            entry.partition = partition;
            entry.objectId = key.objectId;
            entry.mostRecentOffset = offset;
            kvStore.put(storeKey, new Bytes(entry.toBytes()));

        }
    }
}
