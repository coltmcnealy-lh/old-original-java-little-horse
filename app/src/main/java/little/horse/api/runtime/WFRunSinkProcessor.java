package little.horse.api.runtime;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import little.horse.common.objects.rundata.WFRun;

public class WFRunSinkProcessor implements Processor<
    String, CoordinatorOutput, String, WFRun
> {
    private ProcessorContext<String, WFRun> context;

    @Override
    public void init(final ProcessorContext<String, WFRun> context) {
        this.context = context;
    }

    @Override
    public void process(final Record<String, CoordinatorOutput> record) {
        CoordinatorOutput o = record.value();

        if (o != null && o.wfRun != null) {
            context.forward(new Record<String, WFRun>(
                record.key(),
                o.wfRun,
                record.timestamp()
            ));
        }
    }
}
