package little.horse.api.metadata;

import little.horse.common.objects.BaseSchema;

public class AliasEntry extends BaseSchema implements Comparable<AliasEntry> {
    public String objectId;

    public long firstOffset;
    public long mostRecentOffset;

    @Override
    public int compareTo(AliasEntry oth) {
        if (firstOffset > oth.firstOffset) {
            return (int)(firstOffset - oth.firstOffset);
        } else {
            return -1 * (int)(oth.firstOffset - firstOffset);
        }
    }
}
