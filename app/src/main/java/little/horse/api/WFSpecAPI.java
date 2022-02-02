package little.horse.api;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIResponsePost;
import little.horse.common.Config;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.objects.rundata.LHStatus;

public class WFSpecAPI {
    private Config config;
    private APIStreamsContext streams;

    public WFSpecAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        WFSpecSchema spec;
        try {
            spec = ctx.bodyAsClass(WFSpecSchema.class);
        } catch(Exception exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError("Bad input value: " + exn.getMessage());
            ctx.json(error);
            return;
        }
        try {
            spec.cleanupAndValidate(config);
        } catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError err = new LHAPIError(exn.getMessage());
            ctx.json(err);
            return;
        }
        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = spec.guid;
        response.status = spec.status;
        response.name = spec.name;
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
        schema.setConfig(config);
        
        schema.desiredStatus = LHStatus.REMOVED;
        schema.record();
        ctx.status(202);

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = schema.guid;
        response.name = schema.name;
        response.status = schema.status;

        ctx.json(response);
    }
}
