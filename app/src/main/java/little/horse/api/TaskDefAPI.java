package little.horse.api;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.LHValidationError;
import little.horse.lib.TaskDef.TaskDefSchema;
import little.horse.lib.TaskDef.PostTaskDefResponse;
import little.horse.lib.TaskDef.TaskDef;

public class TaskDefAPI {
    private Config config;
    private APIStreamsContext streams;

    public TaskDefAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        TaskDefSchema rawSpec = ctx.bodyAsClass(TaskDefSchema.class);
        TaskDef spec;

        try {
            spec = new TaskDef(rawSpec, this.config);
        }
        catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.json(error);
            return;
        }

        spec.record();

        PostTaskDefResponse response = new PostTaskDefResponse();
        response.guid = spec.getModel().guid;
        response.name = spec.getModel().name;
        ctx.json(response);
    }

    public void get(Context ctx) {
        // ReadOnlyKeyValueStore<String, TaskDefSchema> nStore = streams.getTaskDefNameStore();
        ReadOnlyKeyValueStore<String, TaskDefSchema> gStore = streams.getTaskDefGuidStore();
        String id = ctx.pathParam("nameOrGuid");

        // TaskDefSchema schemaFromName = nStore.get(id);
        // TaskDefSchema schemaFromName = null; // TODO: Fix this.
        TaskDefSchema schemaFromGuid = gStore.get(id);
        // if (schemaFromName != null) {
        //     ctx.json(schemaFromName);
        //     return;
        // }
        if (schemaFromGuid != null) {
            ctx.json(schemaFromGuid);
            return;
        }

        LHAPIError error = new LHAPIError(
            "Could not find TasDef with identifier " + id
        );
        ctx.status(404);
        ctx.json(error);
    }
}
