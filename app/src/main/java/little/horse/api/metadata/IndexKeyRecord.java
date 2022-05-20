package little.horse.api.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.objects.BaseSchema;

public class IndexKeyRecord extends BaseSchema {
    public String key;
    public String value;

    public IndexKeyRecord() {}

    public IndexKeyRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @JsonIgnore
    public String getStoreKey() {
        return key + "___" + value;
    }
}
