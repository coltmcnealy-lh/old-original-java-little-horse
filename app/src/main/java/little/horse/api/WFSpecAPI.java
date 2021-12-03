package little.horse.api;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.WFSpec.PostWFSpecResponse;
import little.horse.lib.WFSpec.WFSpec;
import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecAPI {
    private Config config;
    private APIStreamsContext streams;

    public WFSpecAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        WFSpecSchema rawSpec = ctx.bodyAsClass(WFSpecSchema.class);
        WFSpec spec = new WFSpec(rawSpec, this.config);
        spec.record();

        PostWFSpecResponse response = new PostWFSpecResponse();
        response.guid = spec.getModel().guid;
        response.status = spec.getModel().status;
        response.name = spec.getModel().name;
        ctx.json(response);
    }

    public void get(Context ctx) {
        ReadOnlyKeyValueStore<String, WFSpecSchema> store = streams.getWFSpecStore();
        String wfSpecGuid = ctx.pathParam("guid");

        WFSpecSchema schema = store.get(wfSpecGuid);
        if (schema == null) {
            ctx.status(404);
            return;
        }

        ctx.json(schema);
    }
}
