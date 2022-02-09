package little.horse.api.util;

import little.horse.common.objects.LHRecordMetadata;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.rundata.LHDeployStatus;

public class LHAPIPostResult<T extends CoreMetadata> {
    public T spec;
    public String guid;
    public String name;
    public LHRecordMetadata record;
    public String message;
    public LHDeployStatus status;
}
