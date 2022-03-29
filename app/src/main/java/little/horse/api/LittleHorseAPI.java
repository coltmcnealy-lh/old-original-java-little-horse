package little.horse.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.Javalin;
import io.javalin.http.Context;
import little.horse.api.metadata.CoreMetadataAPI;
import little.horse.api.util.APIStreamsContext;
import little.horse.common.DepInjContext;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.metadata.ExternalEventDef;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.LHRpcRawResponse;
 

public class LittleHorseAPI {
    private Javalin app; 
    private DepInjContext config;
    private Set<CoreMetadataAPI<? extends CoreMetadata>> apis;

    private KafkaStreams streams;

    private <T extends CoreMetadata> void addApi(Class<T> cls) {
        apis.add(
            new CoreMetadataAPI<T>(
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

    public LittleHorseAPI(DepInjContext config, KafkaStreams streams) {
        this.config = config;
        this.streams = streams;
        this.apis = new HashSet<>();

        this.app = Javalin.create(javalinConf -> {
            javalinConf.prefer405over404 = true;
            javalinConf.enableCorsForAllOrigins();
        });

        for (Class<? extends CoreMetadata> cls: Arrays.asList(
            WFSpec.class, TaskDef.class, ExternalEventDef.class, WFRun.class
        )) {
            addApi(cls);
        }

        this.app.get("/storeBytes/{storeName}/{storeKey}", this::getBytesFromStore);
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        this.app.start(5000);
    }
}
