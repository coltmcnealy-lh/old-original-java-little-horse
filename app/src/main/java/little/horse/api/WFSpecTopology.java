package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.kafkaStreamsSerdes.WFSpecSerdes;
import little.horse.lib.schemas.WFSpecSchema;

public class WFSpecTopology {

    public static void addStuff(Topology topology, Config config) {
        WFSpecSerdes serde = new WFSpecSerdes();

        String sourceName = "WFSpec Metadata Events";
        String byGuidProcessorName = "WFSpec Guid Processor";
        String byNameProcessorName = "WFSpec Name Processor";
        String nameKeyedSink = "WFSpec Name Keyed Sink";
        String nameKeyedSource = "WFSpec Name Keyed Source";
        String needAttentionProcessorName = "WFSpec Needs Attention Processor";
        String needAttentionSink = "WFSpec Needs Attention Sink";

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {serde.close();}));

        topology.addSource(
            sourceName,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getWFSpecTopic()
        );

        // This step does two things:
        // 1. Save the WFSpecSchema into the Guid-keyed State Store
        // 2. Produce a re-keyed record to the Name-keyed Kafka Topic
        topology.addProcessor(
            byGuidProcessorName,
            WFSpecByGuidProcessor::new,
            sourceName
        );

        // Sink the re-keyed stream into an intermediate topic
        topology.addSink(
            nameKeyedSink,
            config.getWFSpecNameKeyedTopic(),
            Serdes.String().serializer(),
            serde.serializer(),
            byGuidProcessorName
        );

        // Add a source so we can continue processing
        topology.addSource(
            nameKeyedSource,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getWFSpecNameKeyedTopic()
        );

        topology.addProcessor(
            byNameProcessorName,
            WFSpecByNameProcessor::new,
            nameKeyedSource
        );

        // Now we need to add a processor that just throws all of the WFSpecs that aren't in
        // the desired status onto another topic so a listener can pick them up and process
        // them later.
        topology.addProcessor(
            needAttentionProcessorName,
            WFSpecNeedingAttentionProcessor::new,
            sourceName
        );
        topology.addSink(
            needAttentionSink,
            config.getWFSpecActionsTopic(),
            Serdes.String().serializer(),
            serde.serializer(),
            needAttentionProcessorName
        );

        StoreBuilder<KeyValueStore<String, WFSpecSchema>> guidStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_SPEC_GUID_STORE),
                Serdes.String(),
                serde
        );

        StoreBuilder<KeyValueStore<String, WFSpecSchema>> nameStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_SPEC_NAME_STORE),
                Serdes.String(),
                serde
        );

        // add the state store to our topology and connect it to the "Digital Twin Processor"
        topology.addStateStore(guidStoreBuilder, byGuidProcessorName);
        topology.addStateStore(nameStoreBuilder, byNameProcessorName);

    }

}
