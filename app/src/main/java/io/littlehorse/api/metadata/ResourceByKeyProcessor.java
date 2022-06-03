package io.littlehorse.api.metadata;

import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import io.littlehorse.api.OffsetInfo;
import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.GETable;
import io.littlehorse.common.objects.metadata.LHDeployStatus;
import io.littlehorse.common.objects.metadata.POSTable;

public class ResourceByKeyProcessor<T extends GETable>
implements Processor<String, T, String, IndexEvent> {
    private KeyValueStore<String, Bytes> kvStore;
    private ProcessorContext<String, IndexEvent> context;
    private Class<T> cls;
    private DepInjContext config;

    public ResourceByKeyProcessor(Class<T> cls, DepInjContext config) {
        this.config = config;
        this.cls = cls;
    }

    @Override
    public void init(final ProcessorContext<String, IndexEvent> context) {
        this.kvStore = context.getStateStore(T.getIdStoreName(cls));
        this.context = context;
    }

    @Override
    public void process(final Record<String, T> record) {
        Optional<RecordMetadata> rm = context.recordMetadata();
        RecordMetadata recordMeta = rm.isPresent() ? rm.get() : null;

        try {
            processHelper(record);
        } catch (Exception exn) {
            exn.printStackTrace();
            // In the future, maybe implement a deadletter queue.
        }

        if (recordMeta != null) {
            // Now save the offset.
            OffsetInfo oi = new OffsetInfo(recordMeta, new Date(record.timestamp()));

            String key = OffsetInfo.getKey(recordMeta);
            kvStore.put(key, new Bytes(oi.toBytes()));
        }

    }

    private void processHelper(final Record<String, T> record) throws LHSerdeError {
        T newResource = record.value();
        Bytes b = kvStore.get(record.key());

        ResourceDbEntry entry = b != null ?
            BaseSchema.fromBytes(b.get(), ResourceDbEntry.class, config) :
            null;

        T old = entry != null ?
            BaseSchema.fromString(entry.content, cls, config) :
            null;

        Optional<RecordMetadata> rm = context.recordMetadata();
        RecordMetadata recordMeta = rm.isPresent() ? rm.get() : null;
        Long offset = recordMeta == null ? null : recordMeta.offset();

        if (newResource == null) {
            if (old != null) {
                removeOld(record, old, offset);
            }
        } else {
            updateResource(old, newResource, record, offset);
        }
    }

    private void updateResource(
        T old, T newMeta, final Record<String, T> record, long offset
    ) {
        if (newMeta instanceof POSTable) {
            POSTable p = POSTable.class.cast(newMeta);
            POSTable o = old == null ? null : POSTable.class.cast(old);
            try {
                p.processChange(o);
            } catch(Exception exn) {
                // Maybe we want to do some cleanup? Or just leave that to the caller?
                exn.printStackTrace();
                p.status = LHDeployStatus.ERROR;
                p.statusMessage = "Had a failure when deploying the resource: "
                    + exn.getClass().getCanonicalName() + ":\n" + exn.getMessage() +
                    "\n" + ExceptionUtils.getStackTrace(exn);
            }
        }

        newMeta.updated = new Date(record.timestamp());

        // Store the actual data in the ID store:
        ResourceDbEntry entry = new ResourceDbEntry(newMeta, offset);
        kvStore.put(newMeta.getObjectId(), new Bytes(entry.toBytes()));

        // We need to remove aliases from the old and add from the new.
        Set<IndexRecordKey> newAliases = newMeta.getIndexEntries();
        Set<IndexRecordKey> oldAliases = old == null ? new HashSet<>()
            : old.getIndexEntries();

        for (IndexRecordKey ali: oldAliases) {
            if (!newAliases.contains(ali)) {
                // Need to remove it.
                IndexEvent removeEvent = new IndexEvent(
                    record.key(),
                    ali,
                    offset,
                    IndexOperation.DELETE
                );
                Record<String, IndexEvent> ar = new Record<String, IndexEvent>(
                    ali.partitionKey(),
                    removeEvent,
                    record.timestamp()
                );
                context.forward(ar);
            }
        }

        // Now, create new ones.
        for (IndexRecordKey ali: newAliases) {
            if (!oldAliases.contains(ali)) {
                IndexEvent createAliasEvent = new IndexEvent(
                    record.key(),
                    ali,
                    offset,
                    IndexOperation.CREATE
                );
                Record<String, IndexEvent> ar = new Record<String, IndexEvent>(
                    ali.partitionKey(),
                    createAliasEvent,
                    record.timestamp()
                );
                context.forward(ar);
            }
        }
    }

    private void removeOld(final Record<String, T> record, T old, long offset) {
        // Delete side effects (i.e. k8s deployments) if there are any.
        // This call is idempotent.
        if (old instanceof POSTable) {
            POSTable o = POSTable.class.cast(old);
            try {
                o.remove();
            } catch(LHConnectionError exn) {
                exn.printStackTrace();
                o.status = LHDeployStatus.ERROR;
            }
        }

        // Remove all of the aliases.
        Set<IndexRecordKey> aliases = old.getIndexEntries();
        for (IndexRecordKey ali: aliases) {
            IndexEvent aliasEvent = new IndexEvent(
                record.key(),
                ali,
                offset,
                IndexOperation.DELETE
            );
            Record<String, IndexEvent> ar = new Record<String, IndexEvent>(
                ali.toString(),
                aliasEvent,
                record.timestamp()
            );
            context.forward(ar);
        }

        // Remove from the ID store.
        kvStore.delete(record.key());
    }
}