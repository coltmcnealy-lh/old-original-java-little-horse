package little.horse.api.metadata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.common.Config;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.serdes.LHSerdes;


public class MetadataTopologyBuilder {

    public static<T extends CoreMetadata> void addStuff(
        Topology topology, Config config, Class<T> cls
    ) {
        LHSerdes<T> serde = new LHSerdes<>(cls);

        String sourceName = T.typeName + "Metadata Events";
        String byGuidProcessorName = T.typeName + " Guid Processor";
        String byNameProcessorName = T.typeName + " Name Processor";
        String nameKeyedSink = T.typeName + " Name Keyed Sink";
        String nameKeyedSource = T.typeName + " Name Keyed Source";

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {serde.close();}));

        topology.addSource(
            sourceName,
            Serdes.String().deserializer(),
            serde.deserializer(),
            T.getKafkaTopic(config)
        );

        // This step does two things:
        // 1. Save the WFSpecSchema into the Guid-keyed State Store
        // 2. Produce a re-keyed record to the Name-keyed Kafka Topic
        topology.addProcessor(
            byGuidProcessorName,
            () -> {return new BaseGuidProcessor<T>();},
            sourceName
        );

        // Sink the re-keyed stream into an intermediate topic
        topology.addSink(
            nameKeyedSink,
            T.getNameKeyedKafkaTopic(config),
            Serdes.String().serializer(),
            serde.serializer(),
            byGuidProcessorName
        );

        // Add a source so we can continue processing
        topology.addSource(
            nameKeyedSource,
            Serdes.String().deserializer(),
            serde.deserializer(),
            T.getNameKeyedKafkaTopic(config)
        );

        // Now we need to add a processor that just throws all of the WFSpecs that aren't in
        // the desired status onto another topic so a listener can pick them up and process
        // them later.
        topology.addProcessor(
            byNameProcessorName,
            () -> {return new BaseNameProcessor<T>();},
            sourceName
        );

        // Done with the processing logic; now just add the state stores.
        StoreBuilder<KeyValueStore<String, T>> guidStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(T.getStoreName()),
                Serdes.String(),
                serde
        );

        StoreBuilder<KeyValueStore<String, T>> nameStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(T.getNameStoreName()),
                Serdes.String(),
                serde
        );

        topology.addStateStore(guidStoreBuilder, byGuidProcessorName);
        topology.addStateStore(nameStoreBuilder, byNameProcessorName);
    }
}
