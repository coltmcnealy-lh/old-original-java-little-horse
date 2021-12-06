package little.horse.api.TaskDefProcessors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Constants;
import little.horse.lib.TaskDef.TaskDefSchema;


public class TaskDefByNameProcessor implements Processor<
String, TaskDefSchema, String, TaskDefSchema
>  {
    private KeyValueStore<String, TaskDefSchema> kvStore;
    private ProcessorContext<String, TaskDefSchema> context;

    @Override
    public void init(final ProcessorContext<String, TaskDefSchema> context) {
        kvStore = context.getStateStore(Constants.TASK_DEF_NAME_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, TaskDefSchema> record) {
        TaskDefSchema td = record.value();
        if (td == null) {
            kvStore.delete(record.key());
        } else {
            System.out.println("putting in record: " + record.key() + " " + td.toString());
            kvStore.put(record.key(), td);
        }

        context.forward(record);
    }
}
