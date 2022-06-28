package io.littlehorse.common.model.metadata;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import io.littlehorse.common.model.BaseSchema;


@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name",
    scope = ThreadSpec.class
)
public class ThreadSpec extends BaseSchema {
    public String name;
    public String entrypointNodeName;

    public List<WFRunVariableDef> variableDefs;
    public List<InterruptDef> interruptDefs;

    @JsonManagedReference
    public ArrayList<Edge> edges;

    @JsonBackReference
    public WFSpec wfSpec;

    @JsonManagedReference
    public List<Node> nodes;

    @JsonIgnore
    public Node findNode(String nodeName) {
        for (Node node: nodes) {
            if (node.name.equals(nodeName)) {
                return node;
            }
        }
        return null;
    }

    public WFRunVariableDef findVarDef(String name) {
        for (WFRunVariableDef varDef: variableDefs) {
            if (varDef.variableName.equals(name)) {
                return varDef;
            }
        }
        return null;
    }

    public InterruptDef findIdef(String name) {
        for (InterruptDef iDef: interruptDefs) {
            if (iDef.externalEventName.equals(name)) {
                return iDef;
            }
        }
        return null;
    }
}
