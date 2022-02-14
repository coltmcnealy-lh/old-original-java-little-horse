package little.horse.api.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.objects.BaseSchema;

public class AliasIdentifier extends BaseSchema {
    public String aliasName;
    public String aliasValue;

    public AliasIdentifier() {}

    public AliasIdentifier(String aliasName, String aliasValue) {
        this.aliasName = aliasName;
        this.aliasValue = aliasValue;
    }

    @JsonIgnore
    public String getStoreKey() {
        return aliasName + "___" + aliasValue;
    }
}
