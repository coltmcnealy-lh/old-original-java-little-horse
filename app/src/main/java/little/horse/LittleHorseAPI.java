package little.horse;

import io.javalin.Javalin;
import little.horse.api.WFSpecAPI;
import little.horse.lib.Config;


public class LittleHorseAPI {
    private Javalin app;
    private Config config;
    private WFSpecAPI wfSpecAPI;

    public LittleHorseAPI(Config config) {
        this.config = config;
        this.wfSpecAPI = new WFSpecAPI(this.config);

        this.app = Javalin.create();
        this.app.post("/wfSpec", this.wfSpecAPI::post);
    }

    public void cleanup() {
        // Nothing to do yet.
    }

    public void run() {
        this.app.start(5000);
    }
}
