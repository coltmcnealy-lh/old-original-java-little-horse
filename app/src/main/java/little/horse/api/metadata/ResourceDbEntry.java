package little.horse.api.metadata;

import java.util.Set;

import little.horse.common.LHConfig;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.GETable;

public class ResourceDbEntry extends BaseSchema {
    public Set<IndexRecordKey> aliasIdentifiers;
    public long latestSourceOffset;
    public String content;

    public ResourceDbEntry(GETable meta, long offset) {
        this.content = meta.toString();
        this.aliasIdentifiers = meta.getIndexEntries();
        this.latestSourceOffset = offset;
    }
    
    public ResourceDbEntry(LHConfig config) {}

    public ResourceDbEntry() {}
}
