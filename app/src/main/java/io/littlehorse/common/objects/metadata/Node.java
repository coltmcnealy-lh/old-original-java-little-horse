package io.littlehorse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHValidationError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.Node;
import io.littlehorse.common.util.LHDatabaseClient;
import io.littlehorse.common.util.json.JsonMapKey;

@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name",
    scope = Node.class
)
public class Node extends BaseSchema {
    @JsonMapKey
    public String name;

    public VariableAssignment timeoutSeconds;
    public int numRetries = 0;  // For Retries (:

    public NodeType nodeType;

    private ArrayList<Edge> outgoingEdges;
    private ArrayList<Edge> incomingEdges;

    @JsonManagedReference
    public HashMap<String, VariableAssignment> variables;

    @JsonBackReference
    public ThreadSpec threadSpec;

    public String externalEventDefName;
    public String externalEventDefId;

    public VariableAssignment threadWaitThreadId;
    public String threadSpawnThreadSpecName;

    @JsonManagedReference
    public HashMap<String, VariableMutation> variableMutations;

    public TaskDef taskDef;
    public String taskDefName;
    public String taskDefId;

    // Ignored unless node is of nodeType THROW_EXCEPTION
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
            externalEventDef = LHDatabaseClient.getByNameOrId(
                eedGuid, config, ExternalEventDef.class
            );
        }
        return externalEventDef;
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
    public void validate(LHConfig config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        if (threadSpec == null) {
            throw new RuntimeException("Jackson didn't do its thing");
        }

        if (numRetries < 0) {
            throw new LHValidationError("Can't have negative retries.");
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

        if (nodeType == NodeType.TASK) {
            validateTaskNode(config);
        } else if (nodeType == NodeType.EXTERNAL_EVENT) {
            validateExternalEventNode(config);
        } else if (nodeType == NodeType.SPAWN_THREAD) {
            validateSpawnThreadNode(config);
        } else if (nodeType == NodeType.WAIT_FOR_THREAD) {
            validateWaitForThreadNode(config);
        } else if (nodeType == NodeType.SLEEP) {
            validateSleepNode(config);
        } else if (nodeType == NodeType.NOP) {
            validateNopNode(config);
        }
    }

    private void validateNopNode(LHConfig config)
    throws LHValidationError {
        if (taskDefName != null || externalEventDef != null) {
            throw new LHValidationError(
                "Must have null taskdef and externaleventdef for NOP node"
            );
        }

        // TODO: Later on, validate that there's no variable mutations and such here.
    }

    private void validateTaskNode(LHConfig config)
    throws LHValidationError, LHConnectionError {
        String taskDefKey = (taskDefId == null) ? taskDefName : taskDefId;
        if (taskDefKey == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provides neither taskdef name nor digest!",
                name, threadSpec.name
            ));
        }

        taskDef = LHDatabaseClient.getByNameOrId(
            taskDefKey, config, TaskDef.class
        );

        if (taskDef == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provided task def identifier %s but no such " +
                "task def was found!", name, threadSpec.name, taskDefKey
            ));
        }

        taskDefId = taskDef.getObjectId();
        taskDefName = taskDef.name;
    }

    private void validateExternalEventNode(LHConfig config)
    throws LHValidationError, LHConnectionError {
        String eedKey = (externalEventDefId == null) ?
            externalEventDefName : externalEventDefId;

        if (eedKey == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provides neither externalEventDef name nor digest!",
                name, threadSpec.name
            ));
        }
        externalEventDef = LHDatabaseClient.getByNameOrId(
            eedKey, config, ExternalEventDef.class
        );

        if (externalEventDef == null) {
            throw new LHValidationError(String.format(
                "Node %s on thread %s provided external event def identifier %s " +
                "but no such EE definition was found!", name, threadSpec.name, eedKey
            ));
        }
        externalEventDefName = externalEventDef.name;
        externalEventDefId = externalEventDef.getObjectId();
    }

    private void validateSpawnThreadNode(LHConfig config)
    throws LHValidationError {
        String tname = threadSpawnThreadSpecName;
        if (tname == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + name + " specifies no thread to spawn" +
                " on thread " + threadSpec.name
            );
        }

        if (numRetries > 0) {
            throw new LHValidationError(
                "Can't do retry on Spawn Thread node. `numRetries` must be 0."
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

    private void validateWaitForThreadNode(LHConfig config)
    throws LHValidationError {
        if (threadWaitThreadId == null) {
            throw new LHValidationError(
                "Must provide variable assignment to specify thread to wait for!"
            );
        }

        // if (threadWaitThreadId < 1) {
        //     throw new LHValidationError(
        //         "The `threadId` field on a WAIT_FOR_THREAD node must be greater than"
        //         + " zero because it must refer to a thread to wait for, and we cannot"
        //         + " permit waiting for the root node (id `0`)!"
        //     );
        // }

        // TODO: Throw an error if the WAIT_FOR_THREAD node doesn't come after
        // the SPAWN_THREAD node. Can figure this out by doing analysis on the
        // graph of nodes/edges on the ThreadSpec.
    }

    private void validateSleepNode(LHConfig config) throws LHValidationError {
        // Need to check that the variables specify how long to sleep.

        if (timeoutSeconds == null) {
            throw new LHValidationError(
                "Didn't provide sleepSeconds VariableASsignment var for sleep node!"
            );
        }

        if (numRetries > 0) {
            throw new LHValidationError(
                "Can't do retry on SLEEP node. `numRetries` must be 0."
            );
        }

        // TODO: Once we have some sort of JSONSchema validation, make sure that we
        // are gonna get an INT back.
    }
}
