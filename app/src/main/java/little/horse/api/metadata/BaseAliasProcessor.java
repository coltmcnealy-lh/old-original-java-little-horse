package little.horse.api.metadata;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.Constants;

public class BaseAliasProcessor<T extends CoreMetadata>
implements Processor<String, AliasEvent, Void, Void> {
    private KeyValueStore<String, Bytes> kvStore;
    private Config config;

    public BaseAliasProcessor(Class<T> cls, Config config) {
        // this.cls = cls;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<Void, Void> context) {
        this.kvStore = context.getStateStore(T.getAliasStoreName());
    }

    @Override
    public void process(final Record<String, AliasEvent> record) {
        try {
            processHelper(record);
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            // In the future, maybe implement a deadletter queue.
        }
    }

    private void processHelper(final Record<String, AliasEvent> record) throws LHSerdeError {
        AliasEvent ae = record.value();

        if (ae == null) {
            throw new RuntimeException("WTF?");
        }

        AliasIdentifier ai = ae.identifier;

        String storeKey = ai.getStoreKey();
        if (!storeKey.equals(record.key())) {
            throw new RuntimeException("You done messed up A-A-Ron!");
        }

        Bytes aliasBytes = kvStore.get(storeKey);
        AliasEntryCollection entries = aliasBytes.get() != null ? BaseSchema.fromBytes(
            aliasBytes.get(), AliasEntryCollection.class, config
        ) : null;

        if (ae.operation == AliasOperation.DELETE) {
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

        } else if (ae.operation == AliasOperation.CREATE) {
            if (entries == null) {
                entries = new AliasEntryCollection();
            }

            Integer idx = entries.getIndexForGuid(ae.objectId);
            if (idx != null) {
                throw new RuntimeException(
                    "wtf we're creating an entry for something already there"
                );
            }

            AliasEntry entry = new AliasEntry();
            entry.firstOffset = ae.sourceOffset;
            entry.mostRecentOffset = ae.sourceOffset;
            entry.objectId = ae.objectId;
            entries.entries.add(entry);

            kvStore.put(storeKey, new Bytes(entries.toBytes()));

        } else if (ae.operation == AliasOperation.HEARTBEAT) {
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

            AliasEntry entry = entries.entries.get(idx);
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
