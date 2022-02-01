package little.horse;

import io.javalin.Javalin;
import little.horse.api.APIStreamsContext;
import little.horse.api.ExternalEventDefAPI;
import little.horse.api.TaskDefAPI;
import little.horse.api.WFSpecAPI;
import little.horse.api.WFRunAPI;
import little.horse.lib.Config;
 

public class LittleHorseAPI {
    private Javalin app; 
    private Config config;
    private WFSpecAPI wfSpecAPI;
    private TaskDefAPI taskDefAPI;
    private WFRunAPI wfRunAPI;
    private ExternalEventDefAPI externalEventDefAPI;
    private APIStreamsContext streams;

    public LittleHorseAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
        this.wfSpecAPI = new WFSpecAPI(this.config, this.streams);
        this.taskDefAPI = new TaskDefAPI(this.config, this.streams);
        this.wfRunAPI = new WFRunAPI(this.config, this.streams);
        this.externalEventDefAPI = new ExternalEventDefAPI(config, streams);

        this.app = Javalin.create();

        this.app.post("/wfSpec", this.wfSpecAPI::post);
        this.app.get("/wfSpec/{nameOrGuid}", this.wfSpecAPI::get);
        this.app.delete("/wfSpec/{nameOrGuid}", this.wfSpecAPI::delete);

        this.app.get("/taskDef/{nameOrGuid}", this.taskDefAPI::get);
        this.app.post("/taskDef", this.taskDefAPI::post);

        this.app.get("/wfRun/{wfRunGuid}", this.wfRunAPI::get);
        this.app.post("/wfRun/", this.wfRunAPI::post);
        this.app.post("/wfRun/stop/{wfRunGuid}", this.wfRunAPI::stopWFRun);
        this.app.post("/wfRun/resume/{wfRunGuid}", this.wfRunAPI::resumeWFRun);
        this.app.post("/wfRun/stop/{wfRunGuid}/{tid}", this.wfRunAPI::stopThread);
        this.app.post("/wfRun/resume/{wfRunGuid}/{tid}", this.wfRunAPI::resumeThread);

        this.app.get("/externalEventDef/{nameOrGuid}", this.externalEventDefAPI::get);
        this.app.post("/externalEventDef", this.externalEventDefAPI::post);

        this.app.post("/externalEvent/{externalEventDefID}/{wfRunGuid}", this.externalEventDefAPI::postEvent);
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        this.app.start(5000);
    }
}
