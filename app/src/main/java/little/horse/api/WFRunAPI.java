package little.horse.api;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHValidationError;
import little.horse.lib.WFRun.WFRun;
import little.horse.lib.WFRun.WFRunSchema;
import little.horse.lib.WFSpec.PostWFSpecResponse;
import little.horse.lib.WFSpec.WFSpec;

public class WFRunAPI {
    private Config config;

    public WFRunAPI(Config config) {
        this.config = config;
    }

    public void get(Context ctx) {
        // TODO: Need to query the Collector pod for the WFSpec.
    }

    public void post(Context ctx) {
        WFRunSchema request = ctx.bodyAsClass(WFRunSchema.class);
        String wfSpecId = ctx.pathParam("wfSpec");
        WFSpec wfSpec = null;

        try {
            wfSpec = WFSpec.fromIdentifier(wfSpecId, config);
        } catch (LHLookupException exn) {
            ctx.status(404);
            LHAPIError err = new LHAPIError("Unable to find desired wfSpec: " + exn.getMessage());
            ctx.json(err);
            return;
        } catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError err = new LHAPIError(
                "Failed looking up provided wfSpec: " + exn.getMessage()
            );
            ctx.json(err);
            return;
        }

        WFRun wfRun = new WFRun(request, config, wfSpec);
        wfRun.start();
        PostWFSpecResponse response = new PostWFSpecResponse();
        response.guid = wfRun.getModel().guid;
        response.status = wfRun.getModel().status;

        ctx.status(201);
        ctx.json(response);
    }
}
