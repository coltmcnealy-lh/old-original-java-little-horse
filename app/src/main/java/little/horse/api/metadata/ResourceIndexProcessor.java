package little.horse.api.metadata;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.GETable;
import little.horse.common.util.Constants;

public class ResourceIndexProcessor<T extends GETable>
implements Processor<String, IndexEvent, Void, Void> {
    private KeyValueStore<String, Bytes> kvStore;
    private DepInjContext config;
    private Class<T> cls;

    public ResourceIndexProcessor(Class<T> cls, DepInjContext config) {
        this.cls = cls;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<Void, Void> context) {
        this.kvStore = context.getStateStore(T.getIndexStoreName(cls));
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

    private void processHelper(final Record<String, IndexEvent> record) throws LHSerdeError {
        IndexEvent ae = record.value();

        if (ae == null) {
            throw new RuntimeException("WTF?");
        }

        IndexKeyRecord ai = ae.identifier;

        String storeKey = ai.getStoreKey();
        if (!storeKey.equals(record.key())) {
            throw new RuntimeException("You done messed up A-A-Ron!");
        }

        Bytes aliasBytes = kvStore.get(storeKey);
        IndexEntryCollection entries = aliasBytes != null ? BaseSchema.fromBytes(
            aliasBytes.get(), IndexEntryCollection.class, config
        ) : null;

        if (ae.operation == IndexOperation.DELETE) {
            if (entries == null) {
                throw new RuntimeException(
                    "Shouldn't have call to delete for nonexistent entry."
                );
            }
            Integer idx = entries.getIndexForGuid(ae.objectId);

            if (idx == null) {
                throw new RuntimeException(
                    "How is it possible that the idx is null....?"
                );
            }

            entries.entries.remove(idx.intValue());

            if (entries.entries.size() == 0) {
                kvStore.delete(storeKey);
            } else {
                kvStore.put(storeKey, new Bytes(entries.toBytes()));
            }

        } else if (ae.operation == IndexOperation.CREATE) {
            if (entries == null) {
                entries = new IndexEntryCollection();
            }

            Integer idx = entries.getIndexForGuid(ae.objectId);
            if (idx != null) {
                throw new RuntimeException(
                    "wtf we're creating an entry for something already there"
                );
            }

            IndexEntry entry = new IndexEntry();
            entry.firstOffset = ae.sourceOffset;
            entry.mostRecentOffset = ae.sourceOffset;
            entry.objectId = ae.objectId;
            entries.entries.add(entry);

            kvStore.put(storeKey, new Bytes(entries.toBytes()));

        } else if (ae.operation == IndexOperation.HEARTBEAT) {
            if (entries == null) {
                throw new RuntimeException(
                    "Shouldn't have call to heartbeat for nonexistent entry."
                );
            }

            Integer idx = entries.getIndexForGuid(ae.objectId);

            if (idx == null) {
                throw new RuntimeException(
                    "How is it possible that the idx is null....?"
                );
            }

            IndexEntry entry = entries.entries.get(idx);
            entry.mostRecentOffset = ae.sourceOffset;

            kvStore.put(storeKey, new Bytes(entries.toBytes()));

        } else {
            throw new RuntimeException("What?");
        }

        // I know, this comes from the source to the ID store, but that is intentional.
        kvStore.put(
            Constants.LATEST_OFFSET_ROCKSDB_KEY,
            new Bytes(String.valueOf(ae.sourceOffset).toString().getBytes())
        );
    }
}
