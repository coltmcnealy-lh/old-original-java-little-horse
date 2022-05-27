package little.horse.api.metadata;

import java.util.Date;

import little.horse.common.objects.metadata.GETable;
import little.horse.common.util.LHUtil;

/**
 * This is a naive first attempt at making things searchable in range queries in
 * rocksdb. I've nver written a database query layer, nor have I studied them in
 * detail, so this is likely not optimal...
 */
public class IndexRecordKey {
    public String key;
    public String value;
    public String objectId;
    public Date created;

    public IndexRecordKey() {}

    public static String getLabel(
        String key, String val, Date created, String objectId
    ) {
        return IndexRecordKey.getPartitionKey(key, val) +
            ";;" + LHUtil.dateToDbString(created) + ";;" + objectId;
    }

    public IndexRecordKey(String key, String val, GETable parent) {
        this.objectId = parent.objectId; 
        this.key = key;
        this.value = val;
        this.created = parent.getCreated();
    }

    public IndexRecordKey(String serialized) {
        int lastBreak = serialized.lastIndexOf(";;");
        objectId = serialized.substring(lastBreak + 2);

        serialized = serialized.substring(0, lastBreak);
        lastBreak = serialized.lastIndexOf(";;");
        String dateStr = serialized.substring(lastBreak + 2);
        created = new Date(Long.valueOf(dateStr));

        serialized = serialized.substring(0, lastBreak);
        lastBreak = serialized.lastIndexOf(";;");
        value = serialized.substring(lastBreak + 2);

        key = serialized.substring(0, lastBreak);
    }

    @Override
    public String toString() {
        return getLabel(key, value, created, objectId);
    }

    public static String getPartitionKey(String key, String value) {
        return key + ";;" + value;
    }

    public String partitionKey() {
        return IndexRecordKey.getPartitionKey(key, value);
    }
}
