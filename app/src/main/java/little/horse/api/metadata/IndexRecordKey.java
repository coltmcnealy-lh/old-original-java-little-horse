package little.horse.api.metadata;

import little.horse.common.objects.BaseSchema;

public class IndexRecordKey extends BaseSchema {
    public String label;
    public String objectId;

    public IndexRecordKey() {}

    public IndexRecordKey(String label, String objId) {
        this.label = label;
        this.objectId = objId;
    }

    public IndexRecordKey(String serialized) {
        objectId = serialized.substring(serialized.lastIndexOf(";") + 1);
        label = serialized.substring(0, serialized.lastIndexOf(";"));
    }

    @Override
    public String toString() {
        return this.label + ";" +  this.objectId;
    }
}
