package little.horse.api.util;

import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHStatus;

public class LHAPIResponsePost extends BaseSchema {
    public String name;
    public String guid;
    public LHStatus status;
}