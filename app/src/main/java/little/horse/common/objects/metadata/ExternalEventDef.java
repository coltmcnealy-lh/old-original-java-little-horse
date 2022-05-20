package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.DepInjContext;

// As of now, we don't have any fancy logic here, so it's literally just a CRUD api.
public class ExternalEventDef extends POSTable {
    @JsonIgnore
    public static String typeName = "ExternalEventDef";

    @Override
    public String getObjectId() {
        return this.name;
    }

    @Override
    public void processChange(POSTable other) {
        // Nothing to do I believe.
        if (desiredStatus == LHDeployStatus.RUNNING) {
            status = LHDeployStatus.RUNNING;
        }
    }

    public void validate(DepInjContext config) {
        // Nothing to do yet.
    }
}
