package io.littlehorse.scheduler;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;


public class Scheduler {
    private LHConfig config;
    private StateListener listener;

    public Scheduler(LHConfig config, StateListener listener) {
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
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("hello there");
            schedulerStreams.close();
        }));

        schedulerStreams.start();
    }

    public static void run(LHConfig config) throws LHConnectionError {
        // // TODO: Re-enable kafka streams healthchecks here.
        // if (config.getShouldExposeHealth()) {
        //     Javalin app = LHUtil.createAppWithHealth(listener);
        //     app.start(config.getAdvertisedPort());
        // }

        Scheduler s = new Scheduler(config, null);
        s.run();
    }
}
