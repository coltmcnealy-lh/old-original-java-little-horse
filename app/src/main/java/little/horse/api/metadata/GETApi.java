package little.horse.api.metadata;

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

        // GET /T/{id}
        app.get(T.getAPIPath("/{id}", cls), this::publicGet);

        // GET /search/T/{key}/{value}
        app.get(T.getSearchPath("{key}", "{value}", cls), this::publicSearch);

        // GET /internal/waitFor/T/{id}/{offset}/{partition}
        app.get(
            T.getInternalWaitAPIPath("{id}", "{offset}", "{partition}", cls),
            this::internalWaitForProcessing
        );

        // GET /internal/iter/T/{start}/{end}
        app.get(
            T.getInternalIterLabelsAPIPath("{start}", "{end}", "{token}", cls),
            this::internalIter
        );

        // GET /internal
        app.get(T.getListPath(cls), this::publicList);

        // This code is kind of ugly, but we want the WFRun to have non-standard
        // POST'ing abilities.
        if (cls == WFRun.class) {
            WFRun.overridePostAPIEndpoints(app, config);
        }
    }

    public void publicGet(Context ctx) {
        boolean forceLocal = ctx.queryParamAsClass(
            "forceLocal", Boolean.class
        ).getOrDefault(false);

        String id = ctx.pathParam("id");

        LHRpcResponse<T> response = new LHRpcResponse<>();
        try {
            response.result = streamsContext.getTFromId(id, forceLocal);
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

    public void publicSearch(Context ctx) {
        String key = ctx.pathParam("key");
        String value = ctx.pathParam("value");

        String pastToken = ctx.queryParamAsClass(
            "token", String.class
        ).getOrDefault(null);

        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(30);

        LHRpcResponse<RangeQueryResponse> response = new LHRpcResponse<>();

        try {
            RangeQueryResponse result = streamsContext.search(
                key, value, pastToken, limit
            );
            response.result = result;

            if (response.result == null || response.result.objectIds.size() == 0) {
                response.status = ResponseStatus.OBJECT_NOT_FOUND;
            } else {
                response.status = ResponseStatus.OK;
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

    public void publicList(Context ctx) {
        String pastToken = ctx.queryParamAsClass(
            "token", String.class
        ).getOrDefault(null);

        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(30);
        LHRpcResponse<RangeQueryResponse> response = new LHRpcResponse<>();

        try {
            RangeQueryResponse result = streamsContext.list(
                pastToken, limit
            );
            response.result = result;

        } catch (LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);
        }

        ctx.json(response);

    }

    public void internalIter(Context ctx) {
        LHRpcResponse<RangeQueryResponse> response = new LHRpcResponse<>();
        String start = ctx.pathParam("start");
        String end = ctx.pathParam("end");
        String token = ctx.pathParam("token");
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(30);

        try {
            response.result = streamsContext.iterBetweenKeys(
                start, end, limit, token, true
            );
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);
        }


        ctx.json(response);
    }

    public void internalWaitForProcessing(Context ctx) {
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
                id, offset, partition, forceLocal, T.getInternalWaitAPIPath(
                    id, String.valueOf(offset), String.valueOf(partition), cls
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
}
