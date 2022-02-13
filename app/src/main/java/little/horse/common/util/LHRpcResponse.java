package little.horse.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;

import little.horse.api.ResponseStatus;
import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;

public class LHRpcResponse<T extends BaseSchema> {
    public String message;
    public ResponseStatus status;
    public String id;
    public T result;

    public static<T extends BaseSchema> LHRpcResponse<T> fromResponse(
        byte[] response, Config config, Class<T> cls
    ) throws LHSerdeError {

        RawResult raw = BaseSchema.fromBytes(
            response, RawResult.class, config
        );

        LHRpcResponse<T> out = new LHRpcResponse<>();
        out.status = raw.status;
        out.message = raw.message;
        out.id = raw.id;

        // hackity hack
        try {
            out.result = BaseSchema.fromString(
                LHUtil.mapper.writeValueAsString(raw.result),
                cls,
                config
            );
        } catch (JsonProcessingException exn) {
            throw new LHSerdeError(exn, "unexpected error unwrapping Json");
        }

        return out;
    }
}

class RawResult extends BaseSchema {
    public String message;
    public String id;
    public ResponseStatus status;
    public Object result;
}
