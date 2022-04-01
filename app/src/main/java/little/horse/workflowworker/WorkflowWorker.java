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
        Topology scheduler = new Topology();
        WFSpec wfSpec = ddConfig.lookupWFSpecOrDie(config);

        SchedulerTopology.addStuff(
            scheduler,
            config,
            wfSpec
        );

        KafkaStreams schedulerStreams = new KafkaStreams(
            scheduler, config.getStreamsConfig()
        );
        Runtime.getRuntime().addShutdownHook(new Thread(config::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(schedulerStreams::close));

        schedulerStreams.start();
    }

    public static void main(String[] args) throws LHConnectionError {
        WorkflowWorker dww = new WorkflowWorker(
            new DDConfig(), new DepInjContext()
        );
        dww.run();
    }
}
