package little.horse.common.util;

import little.horse.api.ResponseStatus;
import little.horse.common.objects.BaseSchema;

public class LHRpcRawResponse extends BaseSchema {
    public String message;
    public String objectId;
    public ResponseStatus status;
    public String result;
}
