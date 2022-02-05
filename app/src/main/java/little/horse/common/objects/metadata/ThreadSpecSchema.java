package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
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
        wfSpec = parent;
        setConfig(config);
        for (Map.Entry<String, NodeSchema> p: nodes.entrySet()) {
            NodeSchema node = p.getValue();
            String nodeName = p.getKey();
            node.name = nodeName;
            node.fillOut(config, this);
        }
        // There are no leaf CoreMetadata here, so we go on.

    }
}
