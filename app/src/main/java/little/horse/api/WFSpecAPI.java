package little.horse.api;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.WFSpec.PostWFSpecResponse;
import little.horse.lib.WFSpec.WFSpec;
import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecAPI {
    private Config config;

    public WFSpecAPI(Config config) {
        this.config = config;
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
}
