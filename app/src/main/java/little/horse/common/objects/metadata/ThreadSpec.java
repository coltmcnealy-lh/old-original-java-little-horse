package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import little.horse.common.LHConfig;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.json.JsonMapKey;


@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name",
    scope = ThreadSpec.class
)
public class ThreadSpec extends BaseSchema {
    @JsonMapKey
    public String name;
    public String entrypointNodeName;

    // @JsonManagedReference("vardef-to-thread")
    public HashMap<String, WFRunVariableDef> variableDefs;

    @JsonManagedReference
    public HashMap<String, InterruptDef> interruptDefs;

    @JsonManagedReference
    public ArrayList<Edge> edges;

    @JsonBackReference
    public WFSpec wfSpec;

    @JsonManagedReference
    public HashMap<String, Node> nodes;

    public void validate(LHConfig config, WFSpec parent)
    throws LHValidationError, LHConnectionError {
        setConfig(config);

        wfSpec = parent;
        if (variableDefs == null) {
            variableDefs = new HashMap<String, WFRunVariableDef>();
        }
        if (interruptDefs == null) {
            interruptDefs = new HashMap<String, InterruptDef>();
        }

        if (edges == null) edges = new ArrayList<Edge>();
        for (Edge edge : edges) {
            validateEdge(edge);
        }

        for (Map.Entry<String, Node> p: nodes.entrySet()) {
            Node node = p.getValue();
            String nodeName = p.getKey();
            node.name = nodeName;
            node.validate(config);
        }

        entrypointNodeName = calculateEntrypointNode();
    }

    private void validateEdge(Edge edge) throws LHValidationError {

        if (edge.getSourceNode() == null) {
            throw new LHValidationError(String.format(
                "Edge on thread %s refers to missing node: %s !",
                name, edge.sourceNodeName
            ));
        }

        if (edge.getSinkNode() == null) {
            throw new LHValidationError(String.format(
                "Edge on thread %s refers to missing node: %s !",
                name, edge.sinkNodeName
            ));
        }
    }

    @JsonIgnore
    private String calculateEntrypointNode() throws LHValidationError {
        if (entrypointNodeName != null) {
            if (nodes.get(entrypointNodeName) == null) {
                throw new LHValidationError(
                    "Thread " + name + " has nonexistent entrypoint node " +
                    entrypointNodeName
                );
            }
            return entrypointNodeName;
        }
        Node entrypoint = null;
        for (Map.Entry<String, Node> pair: nodes.entrySet()) {
            Node node = pair.getValue();
            if (node.getIncomingEdges().size() == 0) {
                if (entrypoint != null) {
                    throw new LHValidationError(
                        "Invalid WFSpec: More than one node without incoming edges."
                        );
                    }
                entrypoint = node;
            }
        }
        if (entrypoint == null) {
            throw new LHValidationError(
                "No entrypoint specified and no node present without incoming edges."
            );
        }
        return entrypoint.name;
    }
}
