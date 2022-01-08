package little.horse.lib;

import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.lib.kafkaStreamsSerdes.LHSerdes;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.wfRuntime.WFRuntime;


public class WFRunTopology {

    public static WFRuntime processorFactory(WFEventProcessorActor actor, Config config) {
        return new WFRuntime(actor, config);
    }

    public static void addStuff(
        Topology topology, Config config, Pattern topicPattern, WFEventProcessorActor actor
    ) {

        String topoSource = "WFRun Source";
        String updateProcessorName = "WFRun Update Surfacer";

        LHSerdes<WFEventSchema> eventSerde = new LHSerdes<>(
            WFEventSchema.class
        );
        LHSerdes<WFRunSchema> runSerde = new LHSerdes<>(
            WFRunSchema.class
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            eventSerde.close();
            runSerde.close();
        }));

        topology.addSource(
            topoSource,
            Serdes.String().deserializer(),
            eventSerde.deserializer(),
            topicPattern
        );

        topology.addProcessor(
            updateProcessorName,
            () -> {return WFRunTopology.processorFactory(actor, config);},
            topoSource
        );

        StoreBuilder<KeyValueStore<String, WFRunSchema>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_RUN_STORE),
                Serdes.String(),
                runSerde
            );

        topology.addStateStore(storeBuilder, updateProcessorName);

    }
}
