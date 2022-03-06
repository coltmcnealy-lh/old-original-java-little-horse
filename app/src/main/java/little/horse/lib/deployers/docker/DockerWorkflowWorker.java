package little.horse.lib.deployers.docker;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import little.horse.api.runtime.WFRunTopology;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.LHDatabaseClient;

public class DockerWorkflowWorker {
    private DDConfig ddConfig;
    private Config config;

    public DockerWorkflowWorker(DDConfig ddConfig, Config config) {
        this.ddConfig = ddConfig;
        this.config = config;
    }

    public void run() throws LHConnectionError {
        Topology topology = new Topology();
        WFSpec wfSpec = lookupWFSpecOrDie(ddConfig.getWFSpecId(), config);

        WFRunTopology.addStuff(
            topology,
            config,
            wfSpec
        );

        System.out.println(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology, config.getStreamsConfig(
            config.getAppId()
        ));
        Runtime.getRuntime().addShutdownHook(new Thread(config::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }

    private WFSpec lookupWFSpecOrDie(String id, Config config) {
        WFSpec wfSpec = null;
        try {
            wfSpec = LHDatabaseClient.lookupMetaNameOrId(
                ddConfig.getWFSpecId(), config, WFSpec.class
            );
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
        }
        if (wfSpec == null) {
            throw new RuntimeException("Couldn't load wfSpec");
        }
        return wfSpec;
    }
}
