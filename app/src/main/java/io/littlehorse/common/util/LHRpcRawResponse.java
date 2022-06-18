package io.littlehorse.common.util;

import io.littlehorse.api.ResponseStatus;
import io.littlehorse.common.objects.BaseSchema;

public class LHRpcRawResponse extends BaseSchema {
    public String message;
    public String objectId;
    public ResponseStatus status;
    public String result;
}
