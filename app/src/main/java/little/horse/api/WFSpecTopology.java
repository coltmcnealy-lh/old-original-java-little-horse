package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Config;
import little.horse.lib.WFSpec.WFSpecSchema;
import little.horse.lib.WFSpec.kafkaStreamsSerdes.WFSpecSerdes;

public class WFSpecTopology {
    private Config config;
    private GlobalKTable<String, WFSpecSchema> wfSpecTable;

    public WFSpecTopology(Config config) {
        this.config = config;
    }

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        this.wfSpecTable = builder.globalTable(
            config.getWFSpecTopic(),
            Materialized.<String, WFSpecSchema, KeyValueStore<Bytes, byte[]>>
                as("wf-spec-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(new WFSpecSerdes())
        );

        return builder.build();
    }

    public String getStoreName() {
        return wfSpecTable.queryableStoreName();
    }

}