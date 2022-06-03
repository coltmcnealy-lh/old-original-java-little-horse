package io.littlehorse.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.littlehorse.api.metadata.ApiTopologyBuilder;
import io.littlehorse.api.metadata.GETApi;
import io.littlehorse.api.metadata.POSTApi;
import io.littlehorse.api.util.APIStreamsContext;
import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.metadata.ExternalEventDef;
import io.littlehorse.common.objects.metadata.GETable;
import io.littlehorse.common.objects.metadata.POSTable;
import io.littlehorse.common.objects.metadata.TaskDef;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.objects.rundata.WFRun;
import io.littlehorse.common.util.KStreamsStateListener;
import io.littlehorse.common.util.LHRpcRawResponse;
import io.littlehorse.common.util.LHUtil;
 

public class LittleHorseAPI {
    private Javalin app; 
    private DepInjContext config;
    private Set<GETApi<? extends GETable>> getApis;
    private Set<POSTApi<? extends POSTable>> postApis;
    
    private KafkaStreams streams;
    
    @SuppressWarnings("unchecked")
    public LittleHorseAPI(DepInjContext config, Topology topology) {
        // The API is two components:
        // 1. The Javalin HTTP/REST Frontend
        // 2. The Kafka Streams Backend.

        this.config = config;
        this.getApis = new HashSet<>();
        this.postApis = new HashSet<>();
        
        // Frontend api component
        KStreamsStateListener listener = new KStreamsStateListener();
        this.app = LHUtil.createAppWithHealth(listener);

        // Kafka Streams component
        List<Class<? extends GETable>> resources = Arrays.asList(
            WFSpec.class, TaskDef.class,
            ExternalEventDef.class, WFRun.class
        );
        
        for (Class<? extends GETable> cls: resources) {
            // Initialize the Kafka Streams stuff for that resource type
            ApiTopologyBuilder.addStuff(topology, config, cls);
        }

        this.streams = new KafkaStreams(topology, config.getStreamsConfig());
        this.streams.setStateListener(listener);

        for (Class<? extends GETable> cls: resources) {
            // Now that the backing KafkaStreams is setup, let's add some routes.
            addGETApi(cls);
            if (POSTable.class.isAssignableFrom(cls)) {
                addPOSTApi((Class<? extends POSTable>) cls);
            }
        }

        // Adds a route used by the backend Kafka Streams app for sharded lookups.
        this.app.get(
            "/internal/storeBytes/{storeName}/{storeKey}",
            this::getBytesFromStore
        );
    }

    private <T extends POSTable> void addPOSTApi(Class<T> cls) {
        postApis.add(
            new POSTApi<T>(
                this.config,
                cls,
                new APIStreamsContext<>(streams, cls, config),
                app
            )
        );
    }

    private <T extends GETable> void addGETApi(Class<T> cls) {
        getApis.add(
            new GETApi<T>(
                this.config,
                cls,
                new APIStreamsContext<>(streams, cls, config),
                app
            )
        );
    }

    private void getBytesFromStore(Context ctx) {
        String storeName = ctx.pathParam("storeName");
        String storeKey = ctx.pathParam("storeKey");

        LHRpcRawResponse rawResponse = new LHRpcRawResponse();
        ReadOnlyKeyValueStore<String, Bytes> store = streams.store(
            StoreQueryParameters.fromNameAndType(
                storeName,
                QueryableStoreTypes.keyValueStore()
            )
        );
        Bytes result = store.get(storeKey);
        rawResponse.result = result == null ? null : new String(result.get());
        rawResponse.status = result == null ?
            ResponseStatus.OBJECT_NOT_FOUND : ResponseStatus.OK;
        ctx.json(rawResponse);
    }

    /**
     * Idempotent LittleHorse cluster setup is currently done on startup of the LH
     * Core API. All it does right now is create a bunch of kafka topics so everybody
     * is happy.
     */
    public static void doIdempotentSetup(DepInjContext config) {
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
        
        DepInjContext config = new DepInjContext();
        LHUtil.log("Creating kafka topics");
        LittleHorseAPI.doIdempotentSetup(config);

        LittleHorseAPI lhapi = new LittleHorseAPI(config, new Topology());
        lhapi.run();

    }
}
