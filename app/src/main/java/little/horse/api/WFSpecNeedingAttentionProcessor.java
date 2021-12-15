package little.horse.api;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import little.horse.lib.schemas.WFSpecSchema;

public class WFSpecNeedingAttentionProcessor implements Processor<
String, WFSpecSchema, String, WFSpecSchema
>{
    private ProcessorContext<String, WFSpecSchema> context;

    @Override
    public void init(ProcessorContext<String, WFSpecSchema> context) {
        this.context = context;
    }

    @Override
    public void process(final Record<String, WFSpecSchema> record) {
        WFSpecSchema wfs = record.value();

        if (wfs == null) return;

        if (wfs.status != wfs.desiredStatus) {
            context.forward(record);
        }
    }
}
