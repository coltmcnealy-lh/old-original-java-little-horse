package little.horse.workflowworker;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.lib.deployers.examples.docker.DDConfig;

public class WorkflowWorker {
    private DDConfig ddConfig;
    private DepInjContext config;

    public WorkflowWorker(DDConfig ddConfig, DepInjContext config) {
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
        WorkflowWorker dww = new WorkflowWorker(
            new DDConfig(), new DepInjContext()
        );
        dww.run();
    }
}
