package little.horse.lib.WFRun;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


import little.horse.lib.Config;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.WFSpec.WFSpec;

public class WFRun {
    private WFRunSchema schema;
    private WFSpec wfSpec;
    private Config config;

    private void processSchema() {
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.wfSpecGuid == null) {
            schema.wfSpecGuid = wfSpec.getModel().guid;
        }
        if (schema.wfSpecName == null) {
            schema.wfSpecGuid = wfSpec.getModel().name;
        }

        if (schema.status == null) {
            schema.status = LHStatus.PENDING;
        }
    }

    public WFRun(WFRunSchema schema, Config config) throws LHLookupException, LHValidationError {
        this.config = config;
        this.schema = schema;
        this.wfSpec = getWFSpec();
        this.processSchema();
    }

    public WFSpec getWFSpec() throws LHLookupException, LHValidationError {
        if (schema.wfSpecGuid != null) {
            return WFSpec.fromIdentifier(schema.wfSpecGuid, config);
        } else if (schema.wfSpecName != null) {
            return WFSpec.fromIdentifier(schema.wfSpecName, config);
        }
        throw new LHValidationError(
            "Did not provide wfSpecName nor Guid for wfRun " + this.schema.guid
        );
    }

    public WFRun(WFRunSchema schema, Config config, WFSpec wfSpec) {
        this.config = config;
        this.schema = schema;
        this.wfSpec = wfSpec;
        this.processSchema();
    }

    public WFRunSchema getModel() {
        return this.schema;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String result;
        try {
            result = mapper.writeValueAsString(this.getModel());
        } catch(JsonProcessingException exn) {
            System.out.println(exn.toString());
            result = "Could not serialize.";
        }
        return result;
    }

    public void start() {
    }
}
