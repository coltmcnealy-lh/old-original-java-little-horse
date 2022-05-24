package little.horse.api.metadata;


public class IndexRecordKey {
    public String label;
    public String objectId;

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
