package little.horse.api.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.api.OffsetInfo;
import little.horse.api.OffsetInfoCollection;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.metadata.LHDeployStatus;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;

public class BaseIdProcessor<T extends CoreMetadata>
implements Processor<String, T, String, AliasEvent> {
    private KeyValueStore<String, Bytes> kvStore;
    private ProcessorContext<String, AliasEvent> context;
    private Class<T> cls;
    private Config config;

    public BaseIdProcessor(Class<T> cls, Config config) {
        this.config = config;
        this.cls = cls;
    }

    @Override
    public void init(final ProcessorContext<String, AliasEvent> context) {
        this.kvStore = context.getStateStore(T.getIdStoreName(cls));
        this.context = context;
    }

    @Override
    public void process(final Record<String, T> record) {
        try {
            processHelper(record);
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            // In the future, maybe implement a deadletter queue.
        }
    }

    private void processHelper(final Record<String, T> record) throws LHSerdeError {
        T newMeta = record.value();
        Bytes b = kvStore.get(record.key());

        CoreMetadataEntry entry = b != null ?
            BaseSchema.fromBytes(b.get(), CoreMetadataEntry.class, config) :
            null;

        T old = entry != null ?
            BaseSchema.fromString(entry.content, cls, config) :
            null;

        Optional<RecordMetadata> rm = context.recordMetadata();
        RecordMetadata recordMeta = rm.isPresent() ? rm.get() : null;
        Long offset = recordMeta == null ? null : recordMeta.offset();

        if (newMeta == null) {
            if (old != null) {
                removeOld(record, old, offset);
            }
        } else {
            updateMeta(old, newMeta, record, offset);
        }

        if (recordMeta != null) {
            Bytes nb = kvStore.get(Constants.LATEST_OFFSET_ROCKSDB_KEY);
            OffsetInfoCollection infos = nb != null ? BaseSchema.fromBytes(
                nb.get(), OffsetInfoCollection.class, config
            ) : null;

            if (infos == null) {
                infos = new OffsetInfoCollection();
                infos.partitionMap = new HashMap<String, OffsetInfo>();
            }

            // Now save the offset.
            OffsetInfo oi = new OffsetInfo();
            oi.offset = offset;
            oi.recordTime = new Date(record.timestamp());
            oi.processTime = LHUtil.now();

            infos.latest = oi;
            String key = recordMeta.topic() + recordMeta.partition();
            infos.partitionMap.put(key, oi);

            kvStore.put(Constants.LATEST_OFFSET_ROCKSDB_KEY, new Bytes(infos.toBytes()));
        }
    }

    private void updateMeta(T old, T newMeta, final Record<String, T> record, long offset) {
        if (!newMeta.getId().equals(record.key())) {
            throw new RuntimeException("WTF?");
        }

        // It is somewhat frowned upon to do this within Kafka Streams. However,
        // the processChange is an idempotent, level-triggered method; so repeated
        // calls are not destructive. Furthermore, the deployment of TaskQueue's
        // needn't be super-low latency from when the original API call comes in,
        // because a) creating TaskQueue/deploying WFSpec goes through slow Kafka and
        // Kubernetes API's, and b) the expected throughput of Metadata changes is
        // low, so we don't have to be super fast.
        try {
            newMeta.processChange(old);
        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            newMeta.status = LHDeployStatus.ERROR;
        }

        // Store the actual data in the ID store:
        CoreMetadataEntry entry = new CoreMetadataEntry(newMeta, offset);
        kvStore.put(newMeta.getId(), new Bytes(entry.toBytes()));

        // We need to remove aliases from the old and add from the new.
        Set<AliasIdentifier> newAliases = newMeta.getAliases();
        Set<AliasIdentifier> oldAliases = old == null ? new HashSet<>()
            : old.getAliases();

        int totalAliases = newAliases.size();

        for (AliasIdentifier ali: oldAliases) {
            if (!newAliases.contains(ali)) {
                // Need to remove it.
                AliasEvent removeEvent = new AliasEvent(
                    record.key(),
                    ali,
                    offset,
                    AliasOperation.DELETE,
                    totalAliases
                );
                Record<String, AliasEvent> ar = new Record<String, AliasEvent>(
                    ali.getStoreKey(),
                    removeEvent,
                    record.timestamp()
                );
                context.forward(ar);
            }
        }

        // Now, create new ones.
        for (AliasIdentifier ali: newAliases) {
            if (!oldAliases.contains(ali)) {
                AliasEvent createAliasEvent = new AliasEvent(
                    record.key(),
                    ali,
                    offset,
                    AliasOperation.CREATE,
                    totalAliases
                );
                Record<String, AliasEvent> ar = new Record<String, AliasEvent>(
                    ali.getStoreKey(),
                    createAliasEvent,
                    record.timestamp()
                );
                context.forward(ar);
            }
        }
    }

    private void removeOld(final Record<String, T> record, CoreMetadata old, long offset) {
        // Delete side effects (i.e. k8s deployments) if there are any.
        // This call is idempotent.
        try {
            old.remove();
        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            old.status = LHDeployStatus.ERROR;
        }

        // Remove all of the aliases.
        Set<AliasIdentifier> aliases = old.getAliases();
        int totalAliases = aliases.size();
        for (AliasIdentifier ali: aliases) {
            AliasEvent aliasEvent = new AliasEvent(
                record.key(),
                ali,
                offset,
                AliasOperation.DELETE,
                totalAliases
            );
            Record<String, AliasEvent> ar = new Record<String, AliasEvent>(
                ali.getStoreKey(),
                aliasEvent,
                record.timestamp()
            );
            context.forward(ar);
        }

        // Remove from the ID store.
        kvStore.delete(record.key());
    }
}