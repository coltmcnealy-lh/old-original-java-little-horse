package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import little.horse.common.objects.metadata.Node;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;
import little.horse.common.util.json.JsonMapKey;

@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name"
)
public class Node extends BaseSchema {
    @JsonMapKey
    public String name;

    public NodeType nodeType;
    
    private ArrayList<Edge> outgoingEdges;
    private ArrayList<Edge> incomingEdges;

    public ArrayList<Edge> getOutgoingEdges() {
        if (outgoingEdges == null) {
            outgoingEdges = new ArrayList<>();
            for (Edge edge: threadSpec.edges) {
                if (edge.sourceNodeName == name) {
                    outgoingEdges.add(edge);
                }
            }
        }
        return outgoingEdges;
    }

    public ArrayList<Edge> getIncomingEdges() {
        if (incomingEdges == null) {
            incomingEdges = new ArrayList<>();
            for (Edge edge: threadSpec.edges) {
                if (edge.sinkNodeName == name) {
                    incomingEdges.add(edge);
                }
            }
        }
        return incomingEdges;
    }

    @JsonManagedReference
    public HashMap<String, VariableAssignment> variables;

    @DigestIgnore
    @JsonBackReference
    public ThreadSpec threadSpec;

    public String externalEventDefName;
    public String externalEventDefDigest;

    public String threadWaitSourceNodeName;
    public String threadSpawnThreadSpecName;

    @JsonManagedReference
    public HashMap<String, VariableMutation> variableMutations;

    public TaskDef taskDef;
    public String taskDefName;
    public String taskDefDigest;

    // Ignored unless node is of nodeType THROW_EXCEPTION_TO_PARENT
    public String exceptionToThrow;

    public ExceptionHandlerSpec baseExceptionhandler;

    @JsonManagedReference
    public HashMap<String, ExceptionHandlerSpec> customExceptionHandlers;

    // Everything below doesn't show up in json.
    @JsonIgnore
    private ExternalEventDef externalEventDef;

    public HashSet<String> getNeededVars() {
        HashSet<String> neededVars = new HashSet<String>();
        // first figure out which variables we need as input
        for (VariableAssignment var: this.variables.values()) {
            if (var.wfRunVariableName != null) {
                neededVars.add(var.wfRunVariableName);
            }
        }

        // Now see which variables we need as output
        for (Map.Entry<String, VariableMutation> p:
            this.variableMutations.entrySet()
        ) {
            // Add the variable that gets mutated
            neededVars.add(p.getKey());

            VariableAssignment rhsVarAssign = p.getValue().sourceVariable;
            if (rhsVarAssign != null) {
                if (rhsVarAssign.wfRunVariableName != null) {
                    neededVars.add(rhsVarAssign.wfRunVariableName);
                }
            }
        }
        return neededVars;
    }

    public ExternalEventDef getExeternalEventDef() throws LHConnectionError {
        if (externalEventDef != null) {
            return externalEventDef;
        }

        if (externalEventDefDigest != null || externalEventDefName != null) {
            String eedGuid = (externalEventDefDigest == null) ?
                externalEventDefName : externalEventDefDigest;
            externalEventDef = LHDatabaseClient.lookupMeta(
                eedGuid, config, ExternalEventDef.class
            );
        }
        return externalEventDef;
    }

    @JsonIgnore
    public String getK8sName() {
        return LHUtil.toValidK8sName(threadSpec.wfSpec.getK8sName() + "-" + name);
    }

    @JsonIgnore
    public ExceptionHandlerSpec getHandlerSpec(String exceptionName) {
        if (exceptionName == null) {
            return baseExceptionhandler;
        } else {
            return customExceptionHandlers.get(exceptionName);
        }
    }

    @Override
    public void validate(Config config) throws LHValidationError {
        throw new RuntimeException("Shouldn't be called!");
    }

    public void fillOut(Config config, ThreadSpec parent)
    throws LHValidationError, LHConnectionError {
        setConfig(config);
        threadSpec = parent;

        if (variableMutations == null) {
            variableMutations = new HashMap<String, VariableMutation>();
        }
        if (variables == null) {
            variables = new HashMap<String, VariableAssignment>();
        }
        if (outgoingEdges == null) {
            outgoingEdges = new ArrayList<Edge>();
        }
        if (incomingEdges == null) {
            incomingEdges = new ArrayList<Edge>();
        }

        if (baseExceptionhandler != null) {
            String handlerSpecName = baseExceptionhandler.handlerThreadSpecName;
            if (handlerSpecName != null) {
                ThreadSpec handlerSpec = threadSpec.wfSpec.threadSpecs.get(
                    handlerSpecName
                );
                if (handlerSpec == null) {
                    throw new LHValidationError(
                        "Exception handler on node " + name + " refers to " +
                        "thread spec that doesn't exist: " + handlerSpecName
                    );
                }
                // TODO: Maybe we should enforce "the exception handler thread
                // can't have input variables"
            }
        }

        // We may have some leaf-level CoreMetadata objects here...
        if (nodeType == NodeType.TASK) {
            fillOutTaskNode(config);
        } else if (nodeType == NodeType.EXTERNAL_EVENT) {
            fillOutExternalEventNode(config);
        } else if (nodeType == NodeType.SPAWN_THREAD) {
            fillOutSpawnThreadNode(config);
        } else if (nodeType == NodeType.WAIT_FOR_THREAD) {
            fillOutWaitForThreadNode(config);
        }
    }

    private void fillOutTaskNode(Config config)
    throws LHValidationError, LHConnectionError {
        try {
            taskDef = LHDatabaseClient.lookupOrCreateTaskDef(
                taskDef, taskDefName, taskDefDigest, config
            );
        } catch (Exception exn) {
            String prefix = "Node " + name + ", thread " + threadSpec.name;
            if (exn instanceof LHValidationError) {
                throw new LHValidationError(prefix + exn.getMessage());
            }
            throw exn;
        }

        taskDefDigest = taskDef.getId();
        taskDefName = taskDef.name;
    }

    private void fillOutExternalEventNode(Config config)
    throws LHValidationError, LHConnectionError {
        try {
            externalEventDef = LHDatabaseClient.lookupOrCreateExternalEventDef(
                externalEventDef, externalEventDefName,
                externalEventDefDigest, config
            );
        } catch (Exception exn) {
            String prefix = "Node " + name + ", thread " + threadSpec.name;
            if (exn instanceof LHValidationError) {
                throw new LHValidationError(prefix + exn.getMessage());
            }
            throw exn;
        }

        taskDefDigest = taskDef.getId();
        taskDefName = taskDef.name;
    }

    private void fillOutSpawnThreadNode(Config config)
    throws LHValidationError, LHConnectionError {
        String tname = threadSpawnThreadSpecName;
        if (tname == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + name + " specifies no thread to spawn" +
                " on thread " + threadSpec.name
            );
        }

        ThreadSpec tspec = threadSpec.wfSpec.threadSpecs.get(tname);
        if (tspec == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + name + " on thread spec " + threadSpec.name +
                " specified unknown thread to spawn: " + tname
            );
        }
    }

    private void fillOutWaitForThreadNode(Config config)
    throws LHValidationError, LHConnectionError {
        Node sourceNode = threadSpec.nodes.get(threadWaitSourceNodeName);

        if (sourceNode == null) {
            throw new LHValidationError(
                "Wait for thread node " + name + "has no valid source node " +
                "from the same thread specified."
            );
        }
        if (sourceNode.nodeType != NodeType.SPAWN_THREAD) {
            throw new LHValidationError(
                "Wait For Thread Node " + name + " references a node that is" +
                " not of type SPAWN_THREAD: " + sourceNode.name + ": " +
                sourceNode.nodeType
            );
        }

        // TODO: Throw an error if the WAIT_FOR_THREAD node doesn't come after
        // the SPAWN_THREAD node. Can figure this out by doing analysis on the
        // graph of nodes/edges on the ThreadSpec.

        threadWaitSourceNodeName = sourceNode.name;
    }
}
