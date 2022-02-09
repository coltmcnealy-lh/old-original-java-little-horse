package little.horse.api;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIResponsePost;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.LHDeployStatus;

public class WFSpecAPI {
    private Config config;
    private APIStreamsContext streams;

    public WFSpecAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        WFSpec spec;
        try {
            spec = ctx.bodyAsClass(WFSpec.class);
        } catch(Exception exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError("Bad input value: " + exn.getMessage());
            ctx.json(error);
            return;
        }
        
        try {
            spec.validate(config);
        } catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError err = new LHAPIError(exn.getMessage());
            ctx.json(err);
            return;
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
            LHAPIError error = new LHAPIError("Internal error: " + exn.getMessage());
            ctx.status(500);
            ctx.json(error);
            return;
        }

        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.digest = spec.getDigest();
        response.status = spec.status;
        response.name = spec.name;
        ctx.json(response);
    }

    private WFSpec getFromIdentifier(String wfSpecId) {
        WFSpec schema = streams.getWFSpecGuidStore().get(wfSpecId);
        if (schema != null) {
            return schema;
        }

        schema = streams.getWFSpecNameStore().get(wfSpecId);
        return schema;
    }

    public void get(Context ctx) {
        String wfSpecId = ctx.pathParam("nameOrGuid");

        WFSpec schema = getFromIdentifier(wfSpecId);
        if (schema != null) {
            ctx.json(schema);
            return;
        }

        ctx.status(404);
        return;
    }

    public void delete(Context ctx) {
        String wfSpecId = ctx.pathParam("nameOrGuid");

        WFSpec schema = getFromIdentifier(wfSpecId);
        
        if (schema == null) {
            ctx.status(404);
            return;
        }
        schema.setConfig(config);
        
        schema.desiredStatus = LHDeployStatus.REMOVED;
        schema.record();
        ctx.status(202);

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.digest = schema.getDigest();
        response.name = schema.name;
        response.status = schema.status;

        ctx.json(response);
    }
}
