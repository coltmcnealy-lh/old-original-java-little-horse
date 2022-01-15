package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.lib.LHStatus;

public class WFSpecSchema extends BaseSchema {
    public String name;
    public String guid;
    public LHStatus status;
    public String kafkaTopic;
    public String entrypointThreadName;
    public LHStatus desiredStatus;

    public HashMap<String, ThreadSpecSchema> threadSpecs;

    @JsonIgnore
    public ArrayList<Map.Entry<String, NodeSchema>> allNodePairs() {
        ArrayList<Map.Entry<String, NodeSchema>> out = new ArrayList<>();
        for (Map.Entry<String, ThreadSpecSchema> tp: threadSpecs.entrySet()) {
            ThreadSpecSchema t = tp.getValue();
            for (Map.Entry<String, NodeSchema> np: t.nodes.entrySet()) {
                out.add(np);
            }
        }
        return out;
    }
}
