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

import little.horse.common.DepInjContext;
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

    @JsonManagedReference
    public HashMap<String, VariableAssignment> variables;

    @DigestIgnore
    @JsonBackReference
    public ThreadSpec threadSpec;

    public String externalEventDefName;
    public String externalEventDefId;

    public String threadWaitSourceNodeName;
    public String threadSpawnThreadSpecName;

    @JsonManagedReference
    public HashMap<String, VariableMutation> variableMutations;

    public TaskDef taskDef;
    public String taskDefName;
    public String taskDefId;

    // Ignored unless node is of nodeType THROW_EXCEPTION_TO_PARENT
    public String exceptionToThrow;

    public ExceptionHandlerSpec baseExceptionhandler;

    @JsonManagedReference
    public HashMap<String, ExceptionHandlerSpec> customExceptionHandlers;

    public ArrayList<Edge> getOutgoingEdges() {
        if (outgoingEdges == null) {
            outgoingEdges = new ArrayList<>();
            for (Edge edge: threadSpec.edges) {
                if (edge.sourceNodeName.equals(name)) {
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
                if (edge.sinkNodeName.equals(name)) {
                    incomingEdges.add(edge);
                }
            }
        }
        return incomingEdges;
    }

    // Everything below doesn't show up in json.
    @JsonIgnore
    private ExternalEventDef externalEventDef;

    @JsonIgnore
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

    @JsonIgnore
    public ExternalEventDef getExeternalEventDef() throws LHConnectionError {
        if (externalEventDef != null) {
            return externalEventDef;
        }

        if (externalEventDefId != null || externalEventDefName != null) {
            String eedGuid = (externalEventDefId == null) ?
                externalEventDefName : externalEventDefId;
            externalEventDef = LHDatabaseClient.lookupMetaNameOrId(
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
    public void validate(DepInjContext config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        if (threadSpec == null) {
            throw new RuntimeException("Jackson didn't do its thing");
        }

        // Just clear this to make sure we don't get anything polluted.
        outgoingEdges = null;
        incomingEdges = null;

        if (variableMutations == null) {
            variableMutations = new HashMap<String, VariableMutation>();
        }
        if (variables == null) {
            variables = new HashMap<String, VariableAssignment>();
        }

        if (baseExceptionhandler != null) {
            String handlerSpecName = baseExceptionhandler.handlerThreadSpecName;
            if (handlerSpecName != null) {
                if (!threadSpec.wfSpec.threadSpecs.containsKey(handlerSpecName)) {
                    throw new LHValidationError(
                        "Exception handler on node " + name + " refers to " +
                        "thread spec that doesn't exist: " + handlerSpecName
                    );
                }
                // Maybe in the future we should enforce "the exception handler thread
                // can't have input variables"?
            }
        }

        // We may have some leaf-level CoreMetadata objects here...
        if (nodeType == NodeType.TASK) {
            validateTaskNode(config);
        } else if (nodeType == NodeType.EXTERNAL_EVENT) {
            validateExternalEventNode(config);
        } else if (nodeType == NodeType.SPAWN_THREAD) {
            validateSpawnThreadNode(config);
        } else if (nodeType == NodeType.WAIT_FOR_THREAD) {
            validateWaitForThreadNode(config);
        }
    }

    private void validateTaskNode(DepInjContext config)
    throws LHValidationError, LHConnectionError {
        String taskDefKey = (taskDefId == null) ? taskDefName : taskDefId;
        if (taskDefKey == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provides neither taskdef name nor digest!",
                name, threadSpec.name
            ));
        }

        taskDef = LHDatabaseClient.lookupMetaNameOrId(
            taskDefKey, config, TaskDef.class
        );

        if (taskDef == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provided task def identifier %s but no such " +
                "task def was found!", name, threadSpec.name, taskDefKey
            ));
        }

        taskDefId = taskDef.getId();
        taskDefName = taskDef.name;
    }

    private void validateExternalEventNode(DepInjContext config)
    throws LHValidationError, LHConnectionError {
        String eedKey = (externalEventDefId == null) ?
            externalEventDefName : externalEventDefId;

        if (eedKey == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provides neither externalEventDef name nor digest!",
                name, threadSpec.name
            ));
        }
        externalEventDef = LHDatabaseClient.lookupMetaNameOrId(
            eedKey, config, ExternalEventDef.class
        );

        if (externalEventDef == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provided external event def identifier %s " +
                "but no such EE definition was found!", name, threadSpec.name, eedKey
            ));
        }
        externalEventDefName = externalEventDef.name;
        externalEventDefId = externalEventDef.getId();
    }

    private void validateSpawnThreadNode(DepInjContext config)
    throws LHValidationError {
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

    private void validateWaitForThreadNode(DepInjContext config)
    throws LHValidationError {
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
