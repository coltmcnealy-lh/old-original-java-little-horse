package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.DigestIgnore;

public class ThreadSpecSchema extends BaseSchema {
    public String name;
    public HashMap<String, WFRunVariableDefSchema> variableDefs;
    public HashMap<String, InterruptDefSchema> interruptDefs;
    public ArrayList<EdgeSchema> edges;
    public String entrypointNodeName;

    @JsonBackReference
    @DigestIgnore
    public WFSpecSchema wfSpec;

    @JsonManagedReference
    public HashMap<String, NodeSchema> nodes;

    @Override
    public void fillOut(Config config) throws LHValidationError {
        throw new RuntimeException("This shouldn't be called.");
    }

    public void fillOut(Config config, WFSpecSchema parent)
    throws LHValidationError, LHConnectionError {
        setConfig(config);

        wfSpec = parent;
        if (variableDefs == null) {
            variableDefs = new HashMap<String, WFRunVariableDefSchema>();
        }
        if (interruptDefs == null) {
            interruptDefs = new HashMap<String, InterruptDefSchema>();
        }

        if (edges == null) edges = new ArrayList<EdgeSchema>();
        for (EdgeSchema edge : edges) {
            cleanupEdge(edge);
        }

        for (Map.Entry<String, NodeSchema> p: nodes.entrySet()) {
            NodeSchema node = p.getValue();
            String nodeName = p.getKey();
            node.name = nodeName;
            node.fillOut(config, this);
        }

        entrypointNodeName = calculateEntrypointNode();
        // There are no leaf CoreMetadata here, so we go on.
    }

    private void cleanupEdge(EdgeSchema edge) {
        NodeSchema source = nodes.get(edge.sourceNodeName);
        NodeSchema sink = nodes.get(edge.sinkNodeName);

        boolean alreadyHasEdge = false;
        for (EdgeSchema candidate : source.outgoingEdges) {
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
        NodeSchema entrypoint = null;
        for (Map.Entry<String, NodeSchema> pair: nodes.entrySet()) {
            NodeSchema node = pair.getValue();
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
