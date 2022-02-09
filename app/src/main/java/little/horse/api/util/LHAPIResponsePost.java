package little.horse.api.util;

import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHDeployStatus;

public class LHAPIResponsePost extends BaseSchema {
    public String name;
    public String digest;
    public LHDeployStatus status;
}