package io.littlehorse.api.metadata;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.metadata.GETable;
import io.littlehorse.common.objects.rundata.WFRun;

public class GETApi<T extends GETable> {
    private Class<T> cls;

    public GETApi(
        LHConfig config, Class<T> cls, Object context, Javalin app
    ) {
        this.cls = cls;

        // GET /T/{id}
        app.get(T.getAPIPath("/{id}", cls), this::publicGet);

        // GET /list/T
        app.get(T.getListPath(cls), this::publicList);

        // GET /search/T/{key}/{value}
        app.get(T.getSearchPath("{key}", "{value}", cls), this::publicSearch);

        // GET /timeSearch/T?start={start}&end={end}
        app.get(T.getTimeSearchPath(cls), this::publicTimeSearch);

        // GET /rangeSearch/T/{key}?start={start}&end={end}
        app.get(T.getRangeSearchPath("{key}", cls), this::publicRangeSearch);

        // GET /internal/iter/T/{start}/{end}
        app.get(
            T.getInternalIterLabelsAPIPath("{start}", "{end}", "{token}", cls),
            this::internalIter
        );

        // This code is kind of ugly, but we want the WFRun to have non-standard
        // POST'ing abilities.
        if (cls == WFRun.class) {
            WFRun.overridePostAPIEndpoints(app, config);
        }
    }

    public void publicGet(Context ctx) {

    }

    public void publicSearch(Context ctx) {

    }

    public void publicRangeSearch(Context ctx) {

    }

    public void publicTimeSearch(Context ctx) {

    }

    public void publicList(Context ctx) {

    }

    public void internalIter(Context ctx) {

    }
}
