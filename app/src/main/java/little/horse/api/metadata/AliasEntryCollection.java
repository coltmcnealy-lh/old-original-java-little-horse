package little.horse.api.metadata;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.objects.BaseSchema;

public class AliasEntryCollection extends BaseSchema {
    // We may put more stuff here later.

    /**
     * The AliasEntryCollection is stored in RocksDB under the key givemn by the
     * python code f"{aliasName}__{aliasValue}".
     * 
     * It is a collection of backreferences to objects in the Guid store. Every object
     * here has the same value `aliasValue` for the alias `aliasName`.
     */
    public ArrayList<AliasEntry> entries;

    @JsonIgnore
    public AliasEntry getLatestEntry() {
        return (entries.size() > 0) ? entries.get(entries.size() - 1) : null;
    }

    /**
     * For an ID of an object in the Guid store, looks up the AliasEntry in
     * `this.entries` for that object. If an AliasEntry with the matching id is found,
     * then the corresponding index is returned. Else, null is returned.
     * @param id the ID of the object in the Guid store to look up.
     * @return the index of corresponding AliasEntry in Guid Store.
     */
    @JsonIgnore
    public Integer getIndexForGuid(String id) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).objectId.equals(id)) {
                return i;
            }
        }
        return null;
    }
}
