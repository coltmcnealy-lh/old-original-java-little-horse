package little.horse;

import io.javalin.Javalin;
import little.horse.api.APIStreamsContext;
import little.horse.api.WFSpecAPI;
import little.horse.lib.Config;


public class LittleHorseAPI {
    private Javalin app;
    private Config config;
    private WFSpecAPI wfSpecAPI;
    private APIStreamsContext streams;

    public LittleHorseAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
        this.wfSpecAPI = new WFSpecAPI(this.config, this.streams);

        this.app = Javalin.create();
        this.app.post("/wfSpec", this.wfSpecAPI::post);
        this.app.get("/wfSpec/{guid}", this.wfSpecAPI::get);
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        this.app.start(5000);
    }
}
