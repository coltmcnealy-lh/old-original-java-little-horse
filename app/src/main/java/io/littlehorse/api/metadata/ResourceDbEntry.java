package io.littlehorse.api.metadata;

import java.util.Set;

import io.littlehorse.common.DepInjContext;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.GETable;

public class ResourceDbEntry extends BaseSchema {
    public Set<IndexRecordKey> aliasIdentifiers;
    public long latestSourceOffset;
    public String content;

    public ResourceDbEntry(GETable meta, long offset) {
        this.content = meta.toString();
        this.aliasIdentifiers = meta.getIndexEntries();
        this.latestSourceOffset = offset;
    }
    
    public ResourceDbEntry(DepInjContext config) {}

    public ResourceDbEntry() {}
}
