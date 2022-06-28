package io.littlehorse.common.model.wfrun;

import java.util.HashMap;
import java.util.Map;
import io.littlehorse.common.exceptions.VarSubError;
import io.littlehorse.common.model.metadata.VariableValue;
import io.littlehorse.proto.VariableValuePb;
import io.littlehorse.proto.WFRunRequestPb;

public class WFRunRequest {
    private String wfRunId;
    private String wfSpecId;
    private Map<String, VariableValue> variableValues;

    public String getWfRunId() {
        return wfRunId;
    }

    public void setWfRunId(String wfRunId) {
        this.wfRunId = wfRunId;
    }

    public String getWfSpecId() {
        return wfSpecId;
    }

    public void setWfSpecId(String wfSpecId) {
        this.wfSpecId = wfSpecId;
    }

    public Map<String, VariableValue> getVariableValues() {
        return variableValues;
    }

    public void setVariableValues(Map<String, VariableValue> variableValues) {
        this.variableValues = variableValues;
    }

    public static WFRunRequest fromProto(WFRunRequestPb proto) throws VarSubError {
        WFRunRequest out = new WFRunRequest();
        out.setWfRunId(proto.getWfRunId());
        out.setWfSpecId(proto.getWfSpecId());

        Map<String, VariableValue> variableValues = new HashMap<>();
        for (Map.Entry<String, VariableValuePb> e: proto.getVariableValuesMap().entrySet()) {
            variableValues.put(e.getKey(), VariableValue.fromProto(e.getValue()));
        }
        out.setVariableValues(variableValues);
        return out;
    }

    public WFRunRequestPb.Builder toProtoBuilder() {
        WFRunRequestPb.Builder b = WFRunRequestPb.newBuilder();
        b.setWfRunId(wfRunId);
        b.setWfSpecId(wfSpecId);
        if (variableValues != null) {
            for (Map.Entry<String, VariableValue> e: variableValues.entrySet()) {
                b.putVariableValues(
                    e.getKey(),
                    e.getValue().toProtoBuilder().build()
                );
            }
        }
        return b;
    }
}
