package little.horse.api;

import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.api.topology.serdes.LHSerdes;
import little.horse.common.Config;
import little.horse.common.events.WFEventSchema;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.objects.rundata.WFRunSchema;
import little.horse.common.util.Constants;


public class WFRunTopology {

    public static WFRuntime processorFactory(Config config) {
        return new WFRuntime(config);
    }

    public static void addStuff(
        Topology topology, Config config, Pattern topicPattern
    ) {

        String topoSource = "WFRun Source";
        String updateProcessorName = "WFRun Update Surfacer";

        LHSerdes<WFEventSchema> eventSerde = new LHSerdes<>(
            WFEventSchema.class
        );
        LHSerdes<WFRunSchema> runSerde = new LHSerdes<>(
            WFRunSchema.class
        );
        LHSerdes<WFSpecSchema> specSerde = new LHSerdes<>(
            WFSpecSchema.class
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            eventSerde.close();
            runSerde.close();
            specSerde.close();
        }));

        topology.addSource(
            topoSource,
            Serdes.String().deserializer(),
            eventSerde.deserializer(),
            topicPattern
        );

        topology.addProcessor(
            updateProcessorName,
            () -> {return new WFRuntime(config);},
            topoSource
        );

        StoreBuilder<KeyValueStore<String, WFRunSchema>> wfRunStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_RUN_STORE),
                Serdes.String(),
                runSerde
            );

        topology.addStateStore(wfRunStoreBuilder, updateProcessorName);

        StoreBuilder<KeyValueStore<String, WFSpecSchema>> wfSpecStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_SPEC_GUID_STORE),
                Serdes.String(),
                specSerde
            );
        topology.addStateStore(wfSpecStoreBuilder, updateProcessorName);
    }
}
