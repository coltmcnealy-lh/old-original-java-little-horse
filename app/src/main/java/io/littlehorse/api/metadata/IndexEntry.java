package io.littlehorse.api.metadata;

import io.littlehorse.common.objects.BaseSchema;

public class IndexEntry extends BaseSchema {
    public String objectId;

    public Long mostRecentOffset;
    public Integer partition;
}
