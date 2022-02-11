package little.horse.api.metadata;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.LHDatabaseClient;

public class CoreMetadataAPI<T extends CoreMetadata> {
    private Config config;
    private Class<T> cls;
    private APIStreamsContext streamsContext;

    public CoreMetadataAPI(
        Config config, Class<T> cls, APIStreamsContext context) {
        this.config = config;
        this.cls = cls;
        this.streamsContext = context;
    }

    public void get(Context ctx) {
        String identifier = ctx.pathParam("id");

        T result = streamsContext.queryRemoteOrLocal(identifier, cls);

        ctx.json(result);
        return;
    }

    public void post(Context ctx) {
        T t;
        try {
            t = BaseSchema.fromBytes(
                ctx.bodyAsBytes(), this.cls, config
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.status(400);
            ctx.json(error);
            return;
        }

        try {
            t.validate(config);
            RecordMetadata record = t.record().get();
            streamsContext.waitForProcessing(record, cls);
            T out = LHDatabaseClient.lookupMeta(t.getId(), config, cls);
            // TODO: actually throw some stuff.

            throw new RuntimeException("Implement the actual response");

        } catch(LHValidationError exn) {
            ctx.status(exn.getHTTPStatus());
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.json(error);
            return;
        } catch(LHConnectionError exn) {
            LHAPIError err = new LHAPIError(
                "Had connection error while validating: " + exn.getMessage()
            );
            ctx.status(500);
            ctx.json(err);
            return;
        } catch(ExecutionException|InterruptedException exn) {

        }
    }

    public void delete(Context ctx) {

    }

    public Iterable<Object> getAliases() {
        return null;
    }
}
