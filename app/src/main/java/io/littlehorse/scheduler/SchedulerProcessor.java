package io.littlehorse.scheduler;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import io.littlehorse.common.model.wfrun.WFRunEvent;

class SchedulerProcessor implements Processor<String, WFRunEvent, String, Bytes> {
    private ProcessorContext<String, Bytes> ctx;

    @Override
    public void init(ProcessorContext<String, Bytes> ctx) {
        this.ctx = ctx;
    }

    @Override
    public void process(Record<String, WFRunEvent> record) {
        System.out.println(record.key());
        WFRunEvent evt = record.value();

        if (evt.getStartedEvent() != null) {
            System.out.println("Started event.");
        } else if (evt.getCompletedEvent() != null) {
            System.out.println("CompletedEvent");
        } else if (evt.getRunRequest() != null) {
            System.out.println("Run request");
        } else {
            System.out.println("all null ):");
        }
    }
}
