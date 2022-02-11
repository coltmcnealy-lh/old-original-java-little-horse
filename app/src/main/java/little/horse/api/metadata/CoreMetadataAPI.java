package little.horse.api.metadata;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

import io.javalin.Javalin;
import io.javalin.http.Context;
import little.horse.api.ResponseStatus;
import little.horse.api.util.APIStreamsContext;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHRpcResponse;

public class CoreMetadataAPI<T extends CoreMetadata> {
    private Config config;
    private Class<T> cls;
    private APIStreamsContext<T> streamsContext;

    public CoreMetadataAPI(
        Config config, Class<T> cls, APIStreamsContext<T> context, Javalin app
    ) {
        this.config = config;
        this.cls = cls;
        this.streamsContext = context;

        // GET /wfSpec/{id}
        app.get(T.getAPIPath() + "/{id}", this::get);

        // GET /wfSpecAlias
        app.get(T.getAliasPath(), this::getAlias);

        // POST /wfSpec
        app.post(T.getAPIPath(), this::post);

        // DELETE /wfSpec
        app.delete(T.getAPIPath(), this::delete);
    }

    public void get(Context ctx) {

        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);

        String id = ctx.pathParam("id");

        LHRpcResponse<T> response = new LHRpcResponse<>();

        try {
            response.result = getFromId(id, forceLocal);
            if (response.result == null) {
                response.message = "Could not find " + cls.getTypeName() +
                    " with id " + id;
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                ctx.status(400);

            } else {
                response.status = ResponseStatus.OK;
            }
        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);

        }

        ctx.json(response);
    }

    public void post(Context ctx) {
        LHRpcResponse<T> response = new LHRpcResponse<>();

        try {
            T t = BaseSchema.fromBytes(ctx.bodyAsBytes(), this.cls, config);
            t.validate(config);
            RecordMetadata record = t.save().get();
            streamsContext.waitForProcessing(t.getId(), record, false);

            response.result = LHDatabaseClient.lookupMeta(t.getId(), config, cls);
            response.status = ResponseStatus.OK;

        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            response.message = "Failed unmarshaling provided spec: " + exn.getMessage();
            ctx.status(400);
            response.status = ResponseStatus.VALIDATION_ERROR;

        } catch(LHValidationError exn) {
            ctx.status(exn.getHTTPStatus());
            response.message = "Failed validating provided spec: " + exn.getMessage();
            response.status = ResponseStatus.VALIDATION_ERROR;

        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);

        } catch(ExecutionException|InterruptedException exn) {
            exn.printStackTrace();
        }

        ctx.json(response);
    }

    public void delete(Context ctx) {
        String id = ctx.pathParam("id");
        LHRpcResponse<T> result = new LHRpcResponse<>();

        try {
            result.result = getFromId(id, false);
            if (result.result == null) {
                result.status = ResponseStatus.OBJECT_NOT_FOUND;
                result.message = "Could not find " + cls.getTypeName() +
                    " with id " + id;
                ctx.status(404);
            } else {
                RecordMetadata record = T.sendNullRecord(id, config).get();
                streamsContext.waitForProcessing(result.id, record, false);
                result.status = ResponseStatus.OK;
                result.message = "Successfully deleted object";
                // TODO: add way to see if the delete actually worked.
            }
        } catch (LHConnectionError exn) {
            result.message = "Failed looking things up: " + exn.getMessage();
            result.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);
        } catch (InterruptedException | ExecutionException exn) {}

        ctx.json(result);
    }

    public void getAlias(Context ctx) {
        throw new RuntimeException("implement me you lazy *********");
    }

    /**
     * Get a T from RocksDB (the ID store) with the provided ID. If `forceLocal` is
     * set to true, then the underlying query answer is NOT allowed to query other
     * stores--i.e. it must be found on THIS INSTANCE's RocksDB.
     * @param id the id to query for.
     * @param forceLocal whether to restrict the query to the RocksDB instance on
     * this host.
     * @return the T if it is found; otherwise null.
     * @throws LHConnectionError if we have an orzdash.
     */
    private T getFromId(String id, boolean forceLocal) throws LHConnectionError {
        return streamsContext.getTFromId(id, forceLocal);
    }
}
