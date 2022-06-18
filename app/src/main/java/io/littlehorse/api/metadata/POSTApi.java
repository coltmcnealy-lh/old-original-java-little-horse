package io.littlehorse.api.metadata;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.metadata.POSTable;

public class POSTApi<T extends POSTable> {
    private LHConfig config;
    private Class<T> cls;

    public POSTApi(
        LHConfig config, Class<T> cls, Object context, Javalin app
    ) {
        this.config = config;
        this.cls = cls;

        // POST /wfSpec
        app.post(T.getAPIPath(cls), this::post);

        // DELETE /wfSpec
        app.delete(T.getAPIPath("{id}", cls), this::delete);
    }


    public void post(Context ctx) {
    }

    public void delete(Context ctx) {
    }
}
