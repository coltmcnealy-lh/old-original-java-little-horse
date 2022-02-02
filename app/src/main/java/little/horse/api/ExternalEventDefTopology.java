package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.api.topology.serdes.LHSerdes;
import little.horse.common.Config;
import little.horse.common.objects.metadata.ExternalEventDefSchema;
import little.horse.common.util.Constants;

public class ExternalEventDefTopology {

    public static void addStuff(Topology topology, Config config) {
        LHSerdes<ExternalEventDefSchema> serde = new LHSerdes<ExternalEventDefSchema>(
            ExternalEventDefSchema.class
        );

        String byGuidProcessorName = "ExternalEventDef Guid Processor";
        String byNameProcessorName = "ExternalEventDef Name Processor";
        String sourceName = "ExternalEventDef Metadata Events";
        String nameKeyedSink = "ExternalEventDef Name Keyed Sink";
        String nameKeyedSource = "ExternalEventDef Name Keyed Source";

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {serde.close();})
        );

        topology.addSource(
            sourceName,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getExternalEventDefTopic()
        );

        // This step does two things:
        // 1. Save the ExternalEventDefSchema into the Guid-keyed State Store
        // 2. Produce a re-keyed record to the Name-keyed Kafka Topic
        topology.addProcessor(
            byGuidProcessorName,
            ExternalEventDefByGuidProcessor::new,
            sourceName
        );

        // Sink the re-keyed stream into an intermediate topic
        topology.addSink(
            nameKeyedSink,
            config.getExternalEventDefNameKeyedTopic(),
            Serdes.String().serializer(),
            serde.serializer(),
            byGuidProcessorName
        );

        // Add a source so we can continue processing
        topology.addSource(
            nameKeyedSource,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getExternalEventDefNameKeyedTopic()
        );

        topology.addProcessor(
            byNameProcessorName,
            ExternalEventDefByNameProcessor::new,
            nameKeyedSource
        );

        StoreBuilder<KeyValueStore<String, ExternalEventDefSchema>> guidStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.EXTERNAL_EVENT_DEF_GUID_STORE),
                Serdes.String(),
                serde
        );

        StoreBuilder<KeyValueStore<String, ExternalEventDefSchema>> nameStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.EXTERNAL_EVENT_DEF_NAME_STORE),
                Serdes.String(),
                serde
        );

        // add the state store to our topology and connect it to the "Digital Twin Processor"
        topology.addStateStore(guidStoreBuilder, byGuidProcessorName);
        topology.addStateStore(nameStoreBuilder, byNameProcessorName);

    }
}