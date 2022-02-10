package little.horse.api.metadata;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIPostResult;
import little.horse.api.util.LHDeployException;
import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;

public class CoreMetadataAPI<T extends CoreMetadata> {
    private Config config;
    private Class<T> cls;
    private APIStreamsContext streamContext;

    public CoreMetadataAPI(
        Config config, Class<T> cls, APIStreamsContext context) {
        this.config = config;
        this.cls = cls;
        this.streamContext = context;
    }

    public void get(Context ctx) {

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
            LHAPIPostResult<T> result = t.createIfNotExists(
                streamContext
            ); // this should be synchronous.
            ctx.json(result);
        } catch(LHDeployException exn) {
            t.undeploy();
            LHAPIError result = new LHAPIError(exn.getMessage());
            ctx.status(exn.getHttpStatus());
            ctx.json(result);
        }
    }

    public void delete(Context ctx) {

    }


}
