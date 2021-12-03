package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import little.horse.lib.Config;
import little.horse.lib.WFSpec.WFSpecSchema;
import little.horse.lib.WFSpec.kafkaStreamsSerdes.WFSpecSerdes;

public class WFSpecTopology {

    public static Topology build(Config config) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<byte[], WFSpecSchema> wfSpecStream = builder.stream(
            config.getWFSpecTopic(),
            Consumed.with(Serdes.ByteArray(), new WFSpecSerdes())
        );
        wfSpecStream.print(
            Printed.<byte[], WFSpecSchema>toSysOut().withLabel("tweets-stream")
        );

        return builder.build();
    }

}