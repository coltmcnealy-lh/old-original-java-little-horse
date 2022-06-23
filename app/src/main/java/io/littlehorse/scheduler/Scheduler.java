package io.littlehorse.scheduler;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import io.javalin.Javalin;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.util.KStreamsStateListener;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.deployers.examples.common.DeployerConfig;

public class Scheduler {
    private DeployerConfig ddConfig;
    private LHConfig config;
    private KStreamsStateListener listener;

    public Scheduler(
        DeployerConfig ddConfig,
        LHConfig config,
        KStreamsStateListener listener
    ) {
        this.ddConfig = ddConfig;
        this.config = config;
        this.listener = listener;
    }

    public void run() throws LHConnectionError {
        Topology scheduler = new Topology();

        SchedulerTopology.addStuff(scheduler, config);

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
            new DeployerConfig(), new LHConfig(), listener
        );
        LHConfig config = new LHConfig();

        Javalin app = LHUtil.createAppWithHealth(listener);
        if (config.getShouldExposeHealth()) {
            app.start(config.getAdvertisedPort());
        }
        ww.run();
    }
}
