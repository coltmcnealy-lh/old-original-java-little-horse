package little.horse.workflowworker;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import io.javalin.Javalin;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.KStreamsStateListener;
import little.horse.common.util.LHUtil;
import little.horse.lib.deployers.examples.docker.DDConfig;

public class WorkflowWorker {
    private DDConfig ddConfig;
    private DepInjContext config;
    private KStreamsStateListener listener;

    public WorkflowWorker(
        DDConfig ddConfig,
        DepInjContext config,
        KStreamsStateListener listener
    ) {
        this.ddConfig = ddConfig;
        this.config = config;
        this.listener = listener;
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
        schedulerStreams.setStateListener(listener);
        Runtime.getRuntime().addShutdownHook(new Thread(config::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(schedulerStreams::close));

        schedulerStreams.start();
    }

    public static void main(String[] args) throws LHConnectionError {
        KStreamsStateListener listener = new KStreamsStateListener();
        WorkflowWorker ww = new WorkflowWorker(
            new DDConfig(), new DepInjContext(), listener
        );
        DepInjContext config = new DepInjContext();

        Javalin app = LHUtil.createAppWithHealth(listener);
        if (config.getShouldExposeHealth()) {
            app.start(config.getAdvertisedPort());
        }
        ww.run();
    }
}
