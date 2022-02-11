package little.horse.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.streams.KafkaStreams;

import io.javalin.Javalin;
import little.horse.api.metadata.CoreMetadataAPI;
import little.horse.api.util.APIStreamsContext;
import little.horse.common.Config;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.metadata.ExternalEventDef;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.TaskQueue;
import little.horse.common.objects.metadata.WFSpec;
 

public class LittleHorseAPI {
    private Javalin app; 
    private Config config;
    private Set<CoreMetadataAPI<? extends CoreMetadata>> apis;

    private WFRunAPI wfRunAPI;

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

    public LittleHorseAPI(Config config, KafkaStreams streams) {
        this.config = config;
        this.streams = streams;
        this.apis = new HashSet<>();

        
        this.app = Javalin.create(javalinConf -> {
            javalinConf.jsonMapper(new LHJavalinJson(config));
            javalinConf.prefer405over404 = true;
            javalinConf.enableCorsForAllOrigins();
        });
        
        for (Class<? extends CoreMetadata> cls: Arrays.asList(
            WFSpec.class, TaskDef.class, TaskQueue.class, ExternalEventDef.class
        )) {
            addApi(cls);
        }

        this.app.get("/wfRun/{wfRunId}", this.wfRunAPI::get);
        this.app.post("/wfRun/", this.wfRunAPI::post);
        this.app.post("/wfRun/stop/{wfRunId}", this.wfRunAPI::stopWFRun);
        this.app.post("/wfRun/resume/{wfRunId}", this.wfRunAPI::resumeWFRun);
        this.app.post("/wfRun/stop/{wfRunId}/{tid}", this.wfRunAPI::stopThread);
        this.app.post("/wfRun/resume/{wfRunId}/{tid}", this.wfRunAPI::resumeThread);

        // this.app.get("/externalEventDef/{nameOrId}", this.externalEventDefAPI::get);
        // this.app.post("/externalEventDef", this.externalEventDefAPI::post);

        // this.app.post("/externalEvent/{externalEventDefID}/{wfRunId}", this.externalEventDefAPI::postEvent);
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        this.app.start(5000);
    }
}
