package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;

public class ThreadSpecSchema extends BaseSchema {
    public String name;
    public String guid;

    public HashMap<String, WFRunVariableDefSchema> variableDefs;
    public HashMap<String, InterruptDefSchema> interruptDefs;
    @JsonManagedReference
    public HashMap<String, NodeSchema> nodes;
    public ArrayList<EdgeSchema> edges;

    public String entrypointNodeName;

    @JsonBackReference
    public WFSpecSchema wfSpec;

    @JsonIgnore
    @Override
    public Config setConfig(Config config) {
        super.setConfig(config);
        if (nodes == null) nodes = new HashMap<>();
        for (NodeSchema node: nodes.values()) {
            node.setConfig(config);
            node.threadSpec = this;
        }

        if (edges == null) edges = new ArrayList<>();
        for (EdgeSchema edge: edges) {
            edge.setConfig(config);
        }

        return this.config;
    }
}
