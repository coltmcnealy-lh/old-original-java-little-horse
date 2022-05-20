package little.horse.api.metadata;

import java.util.ArrayList;

import io.javalin.Javalin;
import io.javalin.http.Context;
import little.horse.api.ResponseStatus;
import little.horse.api.util.APIStreamsContext;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.GETable;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.LHRpcResponse;

public class GETApi<T extends GETable> {
    private Class<T> cls;
    private APIStreamsContext<T> streamsContext;

    public GETApi(
        DepInjContext config, Class<T> cls, APIStreamsContext<T> context, Javalin app
    ) {
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

        // GET /WFSpecAll
        app.get(T.getAllAPIPath(cls), this::getAll);

        app.get(
            T.getAliasSetPath("{aliasKey}", "{aliasValue}", cls),
            this::getAliasCollection
        );

        // This code is kind of ugly, but we want the WFRun to have non-standard
        // POST'ing abilities.
        if (cls == WFRun.class) {
            WFRun.overridePostAPIEndpoints(app, config);
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
                response.objectId = response.result.getObjectId();
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

    // TODO: in the future, we're gonna want to validate whether the provided alias
    // name (i.e. search key) is valid for this T.
    public void getAlias(Context ctx) {
        String aliasKey = ctx.pathParam("aliasKey");
        String aliasValue = ctx.pathParam("aliasValue");

        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);        

        LHRpcResponse<IndexEntryCollection> response = new LHRpcResponse<>();

        try {
            IndexEntryCollection collection = streamsContext.getTFromAlias(
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

                // AliasEntry entry = collection.entries.get(
                //     collection.entries.size() - 1
                // );

                // response.result = streamsContext.getTFromId(
                //     entry.objectId, forceLocal
                // );
                response.result = collection;
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

    public void getAliasCollection(Context ctx) {
        String aliasKey = ctx.pathParam("aliasKey");
        String aliasValue = ctx.pathParam("aliasValue");

        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);        

        LHRpcResponse<IndexEntryCollection> response = new LHRpcResponse<>();

        try {
            IndexEntryCollection collection = streamsContext.getTFromAlias(
                aliasKey, aliasValue, forceLocal
            );

            if (collection == null) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
                response.message = "No objects found matching search criteria.";
                response.result = new IndexEntryCollection();
                response.result.entries = new ArrayList<>();

            } else {
                if (collection.entries.size() == 0) {
                    throw new RuntimeException(
                        "This shouldn't be possible, see BaseAliasProcessor.java"
                    );
                }

                response.result = collection;
                response.status = ResponseStatus.OK;
                response.objectId = null;
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

    public void getAll(Context ctx) {
        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);

        try {
            ArrayList<String> out = streamsContext.getAllIds(forceLocal);
            ctx.json(out);
        } catch(LHConnectionError exn) {
            ctx.status(500);
        }
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
