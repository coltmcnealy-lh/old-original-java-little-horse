package io.littlehorse.scheduler;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import io.javalin.Javalin;
import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.util.KStreamsStateListener;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.deployers.examples.common.DeployerConfig;

public class Scheduler {
    private DeployerConfig ddConfig;
    private DepInjContext config;
    private KStreamsStateListener listener;

    public Scheduler(
        DeployerConfig ddConfig,
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
        Scheduler ww = new Scheduler(
            new DeployerConfig(), new DepInjContext(), listener
        );
        DepInjContext config = new DepInjContext();

        Javalin app = LHUtil.createAppWithHealth(listener);
        if (config.getShouldExposeHealth()) {
            app.start(config.getAdvertisedPort());
        }
        ww.run();
    }
}
