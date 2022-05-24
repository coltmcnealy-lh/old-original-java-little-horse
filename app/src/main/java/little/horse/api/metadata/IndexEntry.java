package little.horse.api.metadata;

import little.horse.common.objects.BaseSchema;

public class IndexEntry extends BaseSchema {
    public String objectId;

    public Long mostRecentOffset;
    public Integer partition;
}
