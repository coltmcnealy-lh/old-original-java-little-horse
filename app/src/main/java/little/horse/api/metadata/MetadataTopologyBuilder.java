package little.horse.api.metadata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
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
        LHSerdes<T> serde = new LHSerdes<>(cls, config);
        LHSerdes<CoreMetadataEntry> dataSerde = new LHSerdes<>(
            CoreMetadataEntry.class, config
        );
        LHSerdes<AliasEvent> aliasSerde = new LHSerdes<>(
            AliasEvent.class, config
        );

        String theOgSource = T.getLHTypeName(cls) + " Metadata Events";
        String byIdProcessorName = T.getLHTypeName(cls) + " Id Processor";
        String aliasProcessorName = T.getLHTypeName(cls) + " Alias Processor";
        String aliasSink = T.getLHTypeName(cls) + " Alias Sink";
        String aliasSource = T.getLHTypeName(cls) + " Alias Source";

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            serde.close();
            aliasSerde.close();
            dataSerde.close();
        }));

        topology.addSource(
            theOgSource,
            Serdes.String().deserializer(),
            serde.deserializer(),
            T.getIdKafkaTopic(config, cls)
        );

        /*
        This step does three things:
        1. Save the CoreMetadata into the ID-keyed State Store
        2. Store the offset of the last record we processed in the ID-keyed store
        3. Forward zero or more records re-keyed by alias so that we can store
           aliased items in the alias store in later steps.
        */
        topology.addProcessor(
            byIdProcessorName, // name of this processor
            () -> {return new BaseIdProcessor<T>(cls, config);}, // actual processing
            theOgSource // kafka streams source to process
        );

        /*
        Now, you might be wondering, why do we have to throw the aliases into an
        intermediate kafka topic? Why not just store them on the same state store?
        That's because we need to re-partition the records by the alias key, NOT
        by the ID of the thing that the alias refers to. If we stored them on the
        same store/partition as the ID, when the client tries to look up a WFSpec
        with name=foo, they will hash foo() and not the ID of the WFSpec, which means
        the request may or may not get routed to the correct partition, which could
        cause a whole bunch of orzdashes.
        */

        // Sink the re-keyed stream into an intermediate topic
        topology.addSink(
            aliasSink, // name of this sink
            T.getAliasKafkaTopic(config, cls), // kafka topic to sink to
            Serdes.String().serializer(), // key serializer
            aliasSerde.serializer(), // value serializer
            byIdProcessorName // name of the processor to sink into kafka
        );

        // Resume processing from this sink now that we've re-partitioned everything.
        topology.addSource(
            aliasSource,
            Serdes.String().deserializer(),
            aliasSerde.deserializer(), // Picking up CoreMetadataAliases.
            T.getAliasKafkaTopic(config, cls)
        );

        // Now make the aliases queryable in their own store.
        topology.addProcessor(
            aliasProcessorName,
            () -> {return new BaseAliasProcessor<T>(cls, config);},
            aliasSource
        );

        // Done with the processing logic; now just add the state stores.

        // For querying things by ID.
        StoreBuilder<KeyValueStore<String, Bytes>> idStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(T.getIdStoreName(cls)),
                Serdes.String(),
                Serdes.Bytes()
        );

        // For querying things by alias.
        StoreBuilder<KeyValueStore<String, Bytes>> aliasStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(T.getAliasStoreName(cls)),
                Serdes.String(),
                Serdes.Bytes()
        );

        topology.addStateStore(idStoreBuilder, byIdProcessorName);
        topology.addStateStore(aliasStoreBuilder, aliasProcessorName);
    }
}
