package little.horse;

import org.apache.kafka.streams.KafkaStreams;

import io.javalin.Javalin;
import little.horse.api.WFSpecAPI;
import little.horse.lib.Config;


public class LittleHorseAPI {
    private Javalin app;
    private Config config;
    private WFSpecAPI wfSpecAPI;
    private KafkaStreams streams;

    public LittleHorseAPI(Config config, KafkaStreams streams) {
        this.config = config;
        this.wfSpecAPI = new WFSpecAPI(this.config, this.streams);
        this.streams = streams;

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
