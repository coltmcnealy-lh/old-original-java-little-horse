package little.horse.common.util;

import java.io.IOException;

import little.horse.api.ResponseStatus;
import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import okhttp3.Response;

public class LHRpcResponse<T extends CoreMetadata> {
    public String message;
    public ResponseStatus status;
    public String id;
    public T result;

    public static<T extends CoreMetadata> LHRpcResponse<T> fromResponse(
        Response response, Config config, Class<T> cls
    ) throws IOException, LHSerdeError {

        RawResult raw = BaseSchema.fromBytes(
            response.body().bytes(), RawResult.class, config
        );
            
        LHRpcResponse<T> out = new LHRpcResponse<>();
        out.status = raw.status;
        out.message = raw.message;
        out.id = raw.id;

        // hackity hack
        out.result = BaseSchema.fromString(
            LHUtil.mapper.writeValueAsString(raw.result),
            cls,
            config
        );

        return out;
    }
}

class RawResult extends BaseSchema {
    public String message;
    public String id;
    public ResponseStatus status;
    public Object result;
}