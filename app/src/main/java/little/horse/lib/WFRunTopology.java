package little.horse.lib;

import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class WFRunTopology {
    private Config config;
    private Pattern topicPattern;
    
    public WFRunTopology(Config config, Pattern topicPattern) {
        this.config = config;
        this.topicPattern = topicPattern;
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

        )

        return topo;
    }
}
