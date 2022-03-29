package little.horse.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;

import little.horse.api.ResponseStatus;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;

public class LHRpcResponse<T extends BaseSchema> {
    public String message;
    public ResponseStatus status;
    public String objectId;
    public T result;

    public static<T extends BaseSchema> LHRpcResponse<T> fromResponse(
        byte[] response, DepInjContext config, Class<T> cls
    ) throws LHSerdeError {

        LHRpcTempRawResponse raw = BaseSchema.fromBytes(
            response, LHRpcTempRawResponse.class, config
        );

        LHRpcResponse<T> out = new LHRpcResponse<>();
        out.status = raw.status;
        out.message = raw.message;
        out.objectId = raw.objectId;

        // hackity hack
        try {
            out.result = raw.result == null ? null : BaseSchema.fromString(
                LHUtil.getObjectMapper(config).writeValueAsString(raw.result),
                cls,
                config
            );
        } catch (JsonProcessingException exn) {
            throw new LHSerdeError(exn, "unexpected error unwrapping Json");
        }

        return out;
    }
}

class LHRpcTempRawResponse extends LHRpcRawResponse {
    public Object result;
}