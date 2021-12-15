package little.horse.api;


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Constants;
import little.horse.lib.schemas.TaskDefSchema;

public class TaskDefByGuidProcessor
    implements Processor<String, TaskDefSchema, String, TaskDefSchema>
{
    private KeyValueStore<String, TaskDefSchema> kvStore;
    private ProcessorContext<String, TaskDefSchema> context;

    @Override
    public void init(final ProcessorContext<String, TaskDefSchema> context) {
        kvStore = context.getStateStore(Constants.TASK_DEF_GUID_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, TaskDefSchema> record) {
        TaskDefSchema td = record.value();
        if (td == null) {
            kvStore.delete(record.key());
        } else {
            kvStore.put(record.key(), td);
        }

        Record<String, TaskDefSchema> nameKeyedRecord = new Record<>(
            td.name,
            td,
            record.timestamp()
        );
        context.forward(nameKeyedRecord);
    }
}
