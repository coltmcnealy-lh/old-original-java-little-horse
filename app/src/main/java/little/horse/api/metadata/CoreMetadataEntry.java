package little.horse.api.metadata;

import java.util.Set;

import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;

public class CoreMetadataEntry extends BaseSchema {
    public Set<AliasIdentifier> aliasIdentifiers;
    public long latestSourceOffset;
    public String content;

    public CoreMetadataEntry(CoreMetadata meta, long offset) {
        this.content = meta.toString();
        this.aliasIdentifiers = meta.getAliases();
        this.latestSourceOffset = offset;
    }
    
    public CoreMetadataEntry(Config config) {}

    public CoreMetadataEntry() {}
}
