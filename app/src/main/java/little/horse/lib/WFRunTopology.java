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
    private Config config;
    private Pattern topicPattern;
    private WFEventProcessorActor actor;
    
    public WFRunTopology(
        Config config, Pattern topicPattern, WFEventProcessorActor actor
    ) {
        this.config = config;
        this.topicPattern = topicPattern;
        this.actor = actor;
    }

    public WFEventProcessor processorFactory() {
        return new WFEventProcessor(actor, config);
    }

    public Topology getTopology() {
        Topology topo = new Topology();

        String topoSource = "Source";
        String updateProcessorName = "WFRun Update Surfacer";

        WFEventSerdes eventSerde = new WFEventSerdes();
        WFRunSerdes runSerde = new WFRunSerdes();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            eventSerde.close();
            runSerde.close();
        }));
        
        topo.addSource(
            topoSource,
            Serdes.String().deserializer(),
            eventSerde.deserializer(),
            topicPattern
        );

        topo.addProcessor(
            updateProcessorName,
            this::processorFactory,
            topoSource
        );
            
        StoreBuilder<KeyValueStore<String, WFRunSchema>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_RUN_STORE),
                Serdes.String(),
                runSerde
            );

        topo.addStateStore(storeBuilder, updateProcessorName);

        return topo;
    }
}
