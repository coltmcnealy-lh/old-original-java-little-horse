package little.horse.api.runtime;

import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.common.Config;
import little.horse.common.events.WFEvent;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.Constants;
import little.horse.common.util.serdes.LHSerdes;


public class WFRunTopology {

    public static WFRuntime processorFactory(Config config) {
        return new WFRuntime(config);
    }

    public static void addStuff(
        Topology topology, Config config, Pattern topicPattern
    ) {

        String topoSource = "WFRun Source";
        String updateProcessorName = "WFRun Update Surfacer";

        LHSerdes<WFEvent> eventSerde = new LHSerdes<>(
            WFEvent.class, config
        );
        LHSerdes<WFRun> runSerde = new LHSerdes<>(
            WFRun.class, config
        );
        LHSerdes<WFSpec> specSerde = new LHSerdes<>(
            WFSpec.class, config
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

        StoreBuilder<KeyValueStore<String, WFRun>> wfRunStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_RUN_STORE),
                Serdes.String(),
                runSerde
            );

        topology.addStateStore(wfRunStoreBuilder, updateProcessorName);

        StoreBuilder<KeyValueStore<String, WFSpec>> wfSpecStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_SPEC_GUID_STORE),
                Serdes.String(),
                specSerde
            );
        topology.addStateStore(wfSpecStoreBuilder, updateProcessorName);
    }
}
