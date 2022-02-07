package little.horse.api;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIResponsePost;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDefSchema;

public class TaskDefAPI {
    private Config config;
    private APIStreamsContext streams;

    public TaskDefAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        TaskDefSchema spec = ctx.bodyAsClass(TaskDefSchema.class);

        try {
            spec.fillOut(config);
        } catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.json(error);
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
        response.guid = "";
        response.name = "";
        ctx.json(response);
    }

    public void get(Context ctx) {
        ReadOnlyKeyValueStore<String, TaskDefSchema> nStore = streams.getTaskDefNameStore();
        ReadOnlyKeyValueStore<String, TaskDefSchema> gStore = streams.getTaskDefGuidStore();
        String id = ctx.pathParam("nameOrGuid");

        TaskDefSchema schemaFromName = nStore.get(id);
        TaskDefSchema schemaFromGuid = gStore.get(id);
        if (schemaFromName != null) {
            ctx.json(schemaFromName);
            return;
        }
        if (schemaFromGuid != null) {
            ctx.json(schemaFromGuid);
            return;
        }

        LHAPIError error = new LHAPIError(
            "Could not find TaskDef with identifier " + id
        );
        ctx.status(404);
        ctx.json(error);
    }
}
