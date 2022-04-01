package little.horse.workflowworker;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import little.horse.common.objects.metadata.TaskDef;

public class TaskQueueFilterProcessor implements Processor<
    String, CoordinatorOutput, String, TaskScheduleRequest
> {
    private TaskDef tq;
    private ProcessorContext<String, TaskScheduleRequest> context;

    public TaskQueueFilterProcessor(TaskDef tq) {
        this.tq = tq;
    }

    @Override
    public void init(final ProcessorContext<String, TaskScheduleRequest> context) {
        this.context = context;
    }

    @Override
    public void process(final Record<String, CoordinatorOutput> record) {
        CoordinatorOutput o = record.value();

        if (o == null || o.request == null) {
            return;
        }

        if (o.request.taskDefName.equals(tq.name)) {
            context.forward(new Record<String, TaskScheduleRequest>(
                record.key(),
                o.request,
                record.timestamp()
            ));
        }
    }
}
