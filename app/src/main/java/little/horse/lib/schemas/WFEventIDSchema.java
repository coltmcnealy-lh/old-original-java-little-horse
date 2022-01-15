package little.horse.lib.schemas;

import org.apache.kafka.common.record.Record;

import little.horse.lib.Config;

/**
 * Shows how to find the payload of an event in a WFRun's Event History (i.e. a 
 * kafka record's topic, partition, and offset).
 */
public class WFEventIDSchema {
    public String topic;
    public int partition;
    public long offset;

    public Record getRecord(Config config) {
        return null;
    }
}
