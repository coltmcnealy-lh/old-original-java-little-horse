package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.DigestIgnore;
import little.horse.common.util.json.JsonMapKey;


@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name"
)
public class ThreadSpec extends BaseSchema {
    @JsonMapKey
    public String name;
    public String entrypointNodeName;

    @JsonManagedReference
    public HashMap<String, WFRunVariableDef> variableDefs;
    
    @JsonManagedReference
    public HashMap<String, InterruptDef> interruptDefs;
    
    @JsonManagedReference
    public ArrayList<Edge> edges;

    @JsonBackReference
    @DigestIgnore
    public WFSpec wfSpec;

    @JsonManagedReference
    public HashMap<String, Node> nodes;

    public void validate(Config config, WFSpec parent)
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
            cleanupEdge(edge);
        }

        for (Map.Entry<String, Node> p: nodes.entrySet()) {
            Node node = p.getValue();
            String nodeName = p.getKey();
            node.name = nodeName;
            node.fillOut(config, this);
        }

        entrypointNodeName = calculateEntrypointNode();
        // There are no leaf CoreMetadata here, so we go on.
    }

    private void cleanupEdge(Edge edge) {
        Node source = nodes.get(edge.sourceNodeName);
        Node sink = nodes.get(edge.sinkNodeName);

        boolean alreadyHasEdge = false;
        for (Edge candidate : source.outgoingEdges) {
            if (candidate.sinkNodeName.equals(sink.name)) {
                alreadyHasEdge = true;
                break;
            }
        }
        if (!alreadyHasEdge) {
            source.outgoingEdges.add(edge);
            sink.incomingEdges.add(edge);
        }
    }

    @JsonIgnore
    private String calculateEntrypointNode() throws LHValidationError {
        if (entrypointNodeName != null) {
            return entrypointNodeName;
        }
        Node entrypoint = null;
        for (Map.Entry<String, Node> pair: nodes.entrySet()) {
            Node node = pair.getValue();
            if (node.incomingEdges.size() == 0) {
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
