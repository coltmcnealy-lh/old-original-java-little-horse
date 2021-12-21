package little.horse.api;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.LHValidationError;
import little.horse.lib.objects.ExternalEventDef;
import little.horse.lib.schemas.LHAPIResponsePost;
import little.horse.lib.schemas.ExternalEventDefSchema;

public class ExternalEventDefAPI {
    private Config config;
    private APIStreamsContext streams;

    public ExternalEventDefAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        ExternalEventDefSchema rawSpec = ctx.bodyAsClass(ExternalEventDefSchema.class);
        ExternalEventDef spec;

        try {
            spec = new ExternalEventDef(rawSpec, this.config);
        }
        catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.json(error);
            return;
        }

        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = spec.getModel().guid;
        response.name = spec.getModel().name;
        ctx.json(response);
    }

    public void get(Context ctx) {
        ReadOnlyKeyValueStore<String, ExternalEventDefSchema> nStore = streams.getExternalEventDefNameStore();
        ReadOnlyKeyValueStore<String, ExternalEventDefSchema> gStore = streams.getExternalEventDefGuidStore();
        String id = ctx.pathParam("nameOrGuid");

        ExternalEventDefSchema schemaFromName = nStore.get(id);
        ExternalEventDefSchema schemaFromGuid = gStore.get(id);
        if (schemaFromName != null) {
            ctx.json(schemaFromName);
            return;
        }
        if (schemaFromGuid != null) {
            ctx.json(schemaFromGuid);
            return;
        }

        LHAPIError error = new LHAPIError(
            "Could not find ExternalEventDef with identifier " + id
        );
        ctx.status(404);
        ctx.json(error);
    }
}
