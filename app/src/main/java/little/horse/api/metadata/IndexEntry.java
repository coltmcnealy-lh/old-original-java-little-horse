package little.horse.api.metadata;

import little.horse.common.objects.BaseSchema;

public class IndexEntry extends BaseSchema implements Comparable<IndexEntry> {
    public String objectId;

    public long firstOffset;
    public long mostRecentOffset;

    @Override
    public int compareTo(IndexEntry oth) {
        if (firstOffset > oth.firstOffset) {
            return (int)(firstOffset - oth.firstOffset);
        } else {
            return -1 * (int)(oth.firstOffset - firstOffset);
        }
    }
}
