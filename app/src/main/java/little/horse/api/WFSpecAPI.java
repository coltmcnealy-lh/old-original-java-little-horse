package little.horse.api;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.LHStatus;
import little.horse.lib.LHValidationError;
import little.horse.lib.PostWFSpecResponse;
import little.horse.lib.WFSpec;
import little.horse.lib.WFSpecSchema;

public class WFSpecAPI {
    private Config config;
    private APIStreamsContext streams;

    public WFSpecAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        WFSpecSchema rawSpec = ctx.bodyAsClass(WFSpecSchema.class);
        WFSpec spec;
        try {
            spec = new WFSpec(rawSpec, this.config);
        } catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError err = new LHAPIError(exn.getMessage());
            ctx.json(err);
            return;
        }
        spec.record();

        PostWFSpecResponse response = new PostWFSpecResponse();
        response.guid = spec.getModel().guid;
        response.status = spec.getModel().status;
        response.name = spec.getModel().name;
        ctx.json(response);
    }

    private WFSpecSchema getFromIdentifier(String wfSpecId) {
        WFSpecSchema schema = streams.getWFSpecGuidStore().get(wfSpecId);
        if (schema != null) {
            return schema;
        }

        schema = streams.getWFSpecNameStore().get(wfSpecId);
        return schema;
    }

    public void get(Context ctx) {
        String wfSpecId = ctx.pathParam("nameOrGuid");

        WFSpecSchema schema = getFromIdentifier(wfSpecId);
        if (schema != null) {
            ctx.json(schema);
            return;
        }

        ctx.status(404);
        return;
    }

    public void delete(Context ctx) {
        String wfSpecId = ctx.pathParam("nameOrGuid");

        WFSpecSchema schema = getFromIdentifier(wfSpecId);

        if (schema == null) {
            ctx.status(404);
            return;
        }

        // TODO: validate that the action is valid given current status.

        schema.desiredStatus = LHStatus.REMOVED;
        try {
            WFSpec wfSpec = new WFSpec(schema, config);
            wfSpec.record();
            ctx.status(202);

            PostWFSpecResponse response = new PostWFSpecResponse();
            response.guid = schema.guid;
            response.name = schema.name;
            response.status = schema.status;

            ctx.json(response);

        } catch (LHValidationError exn) {
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.status(400);
            ctx.json(error);
            return;
        }

    }
}
