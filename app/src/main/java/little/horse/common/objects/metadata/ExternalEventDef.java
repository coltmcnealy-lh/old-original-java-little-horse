package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.Config;

// As of now, we don't have any fancy logic here, so it's literally just a CRUD api.
public class ExternalEventDef extends CoreMetadata {
    @JsonIgnore
    public static String typeName = "ExternalEventDef";

    @Override
    public void processChange(CoreMetadata other) {
        // Nothing to do I believe.
    }

    public void validate(Config config) {
        // Nothing to do yet.
    }
}
