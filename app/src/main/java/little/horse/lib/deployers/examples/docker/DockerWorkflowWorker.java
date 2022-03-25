package little.horse.lib.deployers.examples.docker;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import little.horse.api.runtime.WFRunTopology;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.WFSpec;

public class DockerWorkflowWorker {
    private DDConfig ddConfig;
    private DepInjContext config;

    public DockerWorkflowWorker(DDConfig ddConfig, DepInjContext config) {
        this.ddConfig = ddConfig;
        this.config = config;
    }

    public void run() throws LHConnectionError {
        Topology topology = new Topology();
        WFSpec wfSpec = ddConfig.lookupWFSpecOrDie(config);

        WFRunTopology.addStuff(
            topology,
            config,
            wfSpec
        );

        System.out.println(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(
            topology, config.getStreamsConfig()
        );
        Runtime.getRuntime().addShutdownHook(new Thread(config::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }

    public static void main(String[] args) throws LHConnectionError {
        DockerWorkflowWorker dww = new DockerWorkflowWorker(
            new DDConfig(), new DepInjContext()
        );
        dww.run();
    }
}
