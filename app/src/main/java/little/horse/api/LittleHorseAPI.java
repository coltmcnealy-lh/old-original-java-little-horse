package little.horse.api;

import io.javalin.Javalin;
import little.horse.api.util.APIStreamsContext;
import little.horse.common.Config;
 

public class LittleHorseAPI {
    private Javalin app; 
    private Config config;
    private WFSpecAPI wfSpecAPI;
    private WFRunAPI wfRunAPI;
    private ExternalEventDefAPI externalEventDefAPI;
    private APIStreamsContext streams;

    public LittleHorseAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
        this.wfSpecAPI = new WFSpecAPI(this.config, this.streams);
        this.wfRunAPI = new WFRunAPI(this.config, this.streams);
        this.externalEventDefAPI = new ExternalEventDefAPI(config, streams);

        this.app = Javalin.create(javalinConf -> {
            javalinConf.jsonMapper(new LHJavalinJson(config));
            javalinConf.prefer405over404 = true;
            javalinConf.enableCorsForAllOrigins();
        });

        this.app.post("/wfSpec", this.wfSpecAPI::post);
        this.app.get("/wfSpec/{nameOrId}", this.wfSpecAPI::get);
        this.app.delete("/wfSpec/{nameOrId}", this.wfSpecAPI::delete);

        // this.app.get("/taskDef/{nameOrId}", this.taskDefAPI::get);
        // this.app.post("/taskDef", this.taskDefAPI::post);

        this.app.get("/wfRun/{wfRunId}", this.wfRunAPI::get);
        this.app.post("/wfRun/", this.wfRunAPI::post);
        this.app.post("/wfRun/stop/{wfRunId}", this.wfRunAPI::stopWFRun);
        this.app.post("/wfRun/resume/{wfRunId}", this.wfRunAPI::resumeWFRun);
        this.app.post("/wfRun/stop/{wfRunId}/{tid}", this.wfRunAPI::stopThread);
        this.app.post("/wfRun/resume/{wfRunId}/{tid}", this.wfRunAPI::resumeThread);

        this.app.get("/externalEventDef/{nameOrId}", this.externalEventDefAPI::get);
        this.app.post("/externalEventDef", this.externalEventDefAPI::post);

        this.app.post("/externalEvent/{externalEventDefID}/{wfRunId}", this.externalEventDefAPI::postEvent);
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        this.app.start(5000);
    }
}
