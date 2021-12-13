package little.horse.lib;

import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.lib.kafkaStreamsSerdes.WFEventSerdes;
import little.horse.lib.kafkaStreamsSerdes.WFRunSerdes;

public class WFRunTopology {

    public static WFEventProcessor processorFactory(WFEventProcessorActor actor, Config config) {
        return new WFEventProcessor(actor, config);
    }

    public static void addStuff(
        Topology topology, Config config, Pattern topicPattern, WFEventProcessorActor actor
    ) {

        String topoSource = "WFRun Source";
        String updateProcessorName = "WFRun Update Surfacer";

        WFEventSerdes eventSerde = new WFEventSerdes();
        WFRunSerdes runSerde = new WFRunSerdes();

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
