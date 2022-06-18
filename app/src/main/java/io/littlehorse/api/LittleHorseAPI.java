package io.littlehorse.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import io.javalin.Javalin;
import io.littlehorse.api.metadata.GETApi;
import io.littlehorse.api.metadata.POSTApi;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.metadata.ExternalEventDef;
import io.littlehorse.common.objects.metadata.GETable;
import io.littlehorse.common.objects.metadata.POSTable;
import io.littlehorse.common.objects.metadata.TaskDef;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.objects.rundata.WFRun;
import io.littlehorse.common.util.KStreamsStateListener;
import io.littlehorse.common.util.LHUtil;
 

public class LittleHorseAPI {
    private Javalin app; 
    private LHConfig config;
    private Set<GETApi<? extends GETable>> getApis;
    private Set<POSTApi<? extends POSTable>> postApis;
    
    private KafkaStreams streams;

    public LittleHorseAPI(LHConfig config, Topology topology) {
        // The API is two components:
        // 1. The Javalin HTTP/REST Frontend
        // 2. The Kafka Streams Backend.

        this.config = config;
        this.getApis = new HashSet<>();
        this.postApis = new HashSet<>();

        // Frontend api component
        KStreamsStateListener listener = new KStreamsStateListener();
        this.app = LHUtil.createAppWithHealth(listener);

    }

    /**
     * Idempotent LittleHorse cluster setup is currently done on startup of the LH
     * Core API. All it does right now is create a bunch of kafka topics so everybody
     * is happy.
     */
    public static void doIdempotentSetup(LHConfig config) {
        int partitions = config.getDefaultPartitions();
        short replicationFactor = (short) config.getDefaultReplicas();

        for (Class<? extends GETable> cls: Arrays.asList(
            WFSpec.class, TaskDef.class, ExternalEventDef.class, WFRun.class
        )) {
            LHUtil.log("About to create topics for ", cls.getName());
            config.createKafkaTopic(
                new NewTopic(
                    POSTable.getIdKafkaTopic(config, cls),
                    partitions,
                    replicationFactor
                )
            );

            config.createKafkaTopic(
                new NewTopic(
                    POSTable.getIndexKafkaTopic(config, cls),
                    partitions,
                    replicationFactor
                )
            );
        }
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(config::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing streams!");
            streams.close();
        }));

        streams.start();
        this.app.start(config.getAdvertisedPort());
    }

    public static void main(String[] args) throws LHConnectionError {
        LHUtil.log("Running the core LittleHorse API");
        
        LHConfig config = new LHConfig();
        LHUtil.log("Creating kafka topics");
        LittleHorseAPI.doIdempotentSetup(config);

        LittleHorseAPI lhapi = new LittleHorseAPI(config, new Topology());
        lhapi.run();

    }
}
