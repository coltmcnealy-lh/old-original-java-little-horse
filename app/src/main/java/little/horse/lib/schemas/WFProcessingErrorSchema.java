package little.horse.lib.schemas;

import little.horse.lib.LHFailureReason;

public class WFProcessingErrorSchema extends BaseSchema {
    public String message;
    public LHFailureReason reason;
}
