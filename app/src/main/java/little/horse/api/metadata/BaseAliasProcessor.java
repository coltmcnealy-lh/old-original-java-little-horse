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
            if (entries == null || !entries.entries.containsKey(ai.aliasValue)) {
                throw new RuntimeException(
                    "Shouldn't have call to delete for nonexistent entry."
                );
            }
            entries.entries.remove(ai.aliasValue);

            if (entries.entries.size() == 0) {
                kvStore.delete(storeKey);
            } else {
                kvStore.put(storeKey, new Bytes(entries.toBytes()));
            }

        } else if (ae.operation == AliasOperation.CREATE) {
            if (entries == null) {
                entries = new AliasEntryCollection();
            }

            AliasEntry entry = entries.entries.get(ai.aliasValue);

            if (entry == null) {
                entry = new AliasEntry();
                entry.firstOffset = ae.sourceOffset;
                entry.mostRecentOffset = ae.sourceOffset;
                entry.id = ae.id;
            }
            entries.entries.put(ai.aliasValue, entry);

            kvStore.put(storeKey, new Bytes(entries.toBytes()));

        } else if (ae.operation == AliasOperation.HEARTBEAT) {
            if (entries == null || !entries.entries.containsKey(ai.aliasValue)) {
                throw new RuntimeException(
                    "Shouldn't have call to heartbeat for nonexistent entry."
                );
            }

            AliasEntry entry = entries.entries.get(ai.aliasValue);
            entry.mostRecentOffset = ae.sourceOffset;

            kvStore.put(storeKey, new Bytes(entries.toBytes()));

        } else {
            throw new RuntimeException("What?");
        }
    }
}
