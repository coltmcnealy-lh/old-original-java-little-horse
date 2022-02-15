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
import little.horse.common.util.LHUtil;

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
        app.get(T.getAPIPath("/{id}", cls), this::get);

        // GET /wfSpecAlias/{aliasKey}/{aliasValue}
        app.get(T.getAliasPath("{aliasKey}", "{aliasValue}", cls), this::getAlias);

        // GET /wfSpecOffset/{id}/{offset}/{partition}
        app.get(
            T.getWaitForAPIPath("{id}", "{offset}", "{partition}", cls),
            this::waitForProcessing
        );

        System.out.println("\n\n\nasdfasdfasdf\n\n\n");

        // A little bit of voodoo to allow for some special overriding stuff, eg for
        // WFRun.
        if (!T.onlyUseDefaultAPIforGET) {
            // POST /wfSpec
            app.post(T.getAPIPath(cls), this::post);

            // DELETE /wfSpec
            app.delete(T.getAPIPath(cls), this::delete);
        } else {
            T.overridePostAPIEndpoints(app, config);
        }
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

            LHUtil.log(t);

            RecordMetadata record = t.save().get();
            streamsContext.waitForProcessing(
                t.getId(), record.offset(), record.partition(), false,
                T.getWaitForAPIPath(
                    t.getId(), record.offset(), record.partition(), cls
                )
            );

            response.result = LHDatabaseClient.lookupMeta(t.getId(), config, cls);
            response.objectId = t.getId();
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
            } else {
                RecordMetadata record = T.sendNullRecord(id, config, cls).get();
                streamsContext.waitForProcessing(
                    result.objectId, record.offset(), record.partition(), false,
                    T.getWaitForAPIPath(
                        id, record.offset(), record.partition(), cls
                    )
                );
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

    // TODO: in the future, we're gonna want to validate whether the provided alias
    // name (i.e. search key) is valid for this T.
    public void getAlias(Context ctx) {
        String aliasKey = ctx.pathParam("aliasKey");
        String aliasValue = ctx.pathParam("aliasValue");

        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);        

        LHRpcResponse<T> response = new LHRpcResponse<>();

        try {
            AliasEntryCollection collection = streamsContext.getTFromAlias(
                aliasKey, aliasValue, forceLocal
            );

            if (collection == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "No objects found matching search criteria.";
            } else {
                if (collection.entries.size() == 0) {
                    throw new RuntimeException(
                        "This shouldn't be possible, see BaseAliasProcessor.java"
                    );
                }

                AliasEntry entry = collection.entries.get(
                    collection.entries.size() - 1
                );

                response.result = streamsContext.getTFromId(entry.objectId, forceLocal);
                if (response.result != null) {
                    response.status = ResponseStatus.OK;
                } else {
                    response.status = ResponseStatus.OBJECT_NOT_FOUND;
                    response.message = "obj deleted and idx will follow soon.";
                }
            }

        } catch (LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);

        }

        ctx.json(response);
    }

    public void waitForProcessing(Context ctx) {
        // TODO: Need to add timeout capabilities to this.

        String id = ctx.pathParam("id");
        long offset = Long.valueOf(ctx.pathParam("offset"));
        int partition = Integer.valueOf(ctx.pathParam("partition"));
        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);

        LHRpcResponse<T> response = new LHRpcResponse<>();

        try {
            streamsContext.waitForProcessing(
                id, offset, partition, forceLocal, T.getWaitForAPIPath(
                    id, offset, partition, cls
                )
            );
            response.status = ResponseStatus.OK;

        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);

        }

        ctx.json(response);
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
