package io.littlehorse.common.objects.metadata;

import java.util.Map;

import io.littlehorse.common.LHConfig;

// As of now, we don't have any fancy logic here, so it's literally just a CRUD api.
public class ExternalEventDef extends POSTable {
    public LHVarType contentType;

    @Override
    public String getObjectId() {
        return this.name;
    }

    public void processChange(POSTable other) {
        // Nothing to do I believe.
        if (desiredStatus == LHDeployStatus.RUNNING) {
            status = LHDeployStatus.RUNNING;
        }
    }

    public void validate(LHConfig config) {
        // Nothing to do yet.
    }

    @Override
    public void addIndexKeyValPairs(Map<String, String> pairs) {}
}
