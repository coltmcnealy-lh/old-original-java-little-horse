package io.littlehorse.common.events;

import org.apache.kafka.common.record.Record;

import io.littlehorse.common.DepInjContext;

/**
 * Shows how to find the payload of an event in a WFRun's Event History (i.e. a 
 * kafka record's topic, partition, and offset).
 */
public class WFEventId {
    public String topic;
    public int partition;
    public long offset;

    public Record getRecord(DepInjContext config) {
        return null;
    }
}
