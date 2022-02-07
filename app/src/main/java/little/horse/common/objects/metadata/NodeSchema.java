package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.NodeSchema;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;

public class NodeSchema extends BaseSchema {
    public String name;
    public NodeType nodeType;
    public String wfSpecGuid;
    public String threadSpecName;

    public ArrayList<EdgeSchema> outgoingEdges;
    public ArrayList<EdgeSchema> incomingEdges;

    public HashMap<String, VariableAssignmentSchema> variables;

    public String externalEventDefName;
    public String externalEventDefGuid;

    public String threadWaitSourceNodeName;
    public String threadWaitSourceNodeGuid;

    public String threadSpawnThreadSpecName;
    public HashMap<String, VariableMutationSchema> variableMutations;

    public TaskDefSchema taskDef;
    public String taskDefName;
    public String taskDefGuid;

    @JsonBackReference
    public ThreadSpecSchema threadSpec;

    // Ignored unless node is of nodeType THROW_EXCEPTION_TO_PARENT
    public String exceptionToThrow;

    public ExceptionHandlerSpecSchema baseExceptionhandler;
    public HashMap<String, ExceptionHandlerSpecSchema> customExceptionHandlers;

    // Everything below doesn't show up in json.
    @JsonIgnore
    private ExternalEventDefSchema externalEventDef;

    @JsonIgnore
    public String getK8sName() {
        return LHUtil.toValidK8sName(threadSpec.wfSpec.k8sName + "-" + name);
    }

    @JsonIgnore
    public ExceptionHandlerSpecSchema getHandlerSpec(String exceptionName) {
        if (exceptionName == null) {
            return baseExceptionhandler;
        } else {
            return customExceptionHandlers.get(exceptionName);
        }
    }

    public TaskDefSchema getTaskDef() {
        return taskDef;
    }

    @JsonIgnore
    public ExternalEventDefSchema getExternalEventDef() {
        if (externalEventDef == null) {
            try {
                externalEventDef = LHDatabaseClient.lookupExternalEventDef(
                    externalEventDefGuid, config
                );
            } catch(LHConnectionError exn) {
                exn.printStackTrace();
            } catch(NullPointerException exn) {
                exn.printStackTrace();
            }
        }
        return externalEventDef;
    }

    @Override
    public void fillOut(Config config) throws LHValidationError {
        throw new RuntimeException("Shouldn't be called!");
    }

    public void fillOut(Config config, ThreadSpecSchema parent)
    throws LHValidationError, LHConnectionError {
        setConfig(config);
        threadSpec = parent;

        if (variableMutations == null) {
            variableMutations = new HashMap<String, VariableMutationSchema>();
        }
        if (variables == null) {
            variables = new HashMap<String, VariableAssignmentSchema>();
        }
        if (outgoingEdges == null) {
            outgoingEdges = new ArrayList<EdgeSchema>();
        }
        if (incomingEdges == null) {
            incomingEdges = new ArrayList<EdgeSchema>();
        }

        if (baseExceptionhandler != null) {
            String handlerSpecName = baseExceptionhandler.handlerThreadSpecName;
            if (handlerSpecName != null) {
                ThreadSpecSchema handlerSpec = threadSpec.wfSpec.threadSpecs.get(
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
                taskDef, taskDefName, taskDefGuid, config
            );
        } catch (Exception exn) {
            String prefix = "Node " + name + ", thread " + threadSpecName;
            if (exn instanceof LHValidationError) {
                throw new LHValidationError(prefix + exn.getMessage());
            } else if (exn instanceof LHConnectionError) {
                throw new LHConnectionError(
                    ((LHConnectionError) exn).parent(),
                    ((LHConnectionError) exn).getReason(),
                    ((LHConnectionError) exn).getMessage()
                );
            }
            throw exn;
        }

        taskDefGuid = taskDef.getDigest();
        taskDefName = taskDef.name;
    }

    private void fillOutExternalEventNode(Config config)
    throws LHValidationError, LHConnectionError {
        try {
            externalEventDef = LHDatabaseClient.lookupOrCreateExternalEventDef(
                externalEventDef, externalEventDefName,
                externalEventDefGuid, config
            );
        } catch (Exception exn) {
            String prefix = "Node " + name + ", thread " + threadSpecName;
            if (exn instanceof LHValidationError) {
                throw new LHValidationError(prefix + exn.getMessage());
            } else if (exn instanceof LHConnectionError) {
                throw new LHConnectionError(
                    ((LHConnectionError) exn).parent(),
                    ((LHConnectionError) exn).getReason(),
                    ((LHConnectionError) exn).getMessage()
                );
            }
            throw exn;
        }

        taskDefGuid = taskDef.getDigest();
        taskDefName = taskDef.name;
    }

    private void fillOutSpawnThreadNode(Config config)
    throws LHValidationError, LHConnectionError {
        String tname = threadSpawnThreadSpecName;
        if (tname == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + name + " specifies no thread to spawn" +
                " on thread " + threadSpecName
            );
        }

        ThreadSpecSchema tspec = threadSpec.wfSpec.threadSpecs.get(tname);
        if (tspec == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + name + " on thread spec " + threadSpecName +
                " specified unknown thread to spawn: " + tname
            );
        }
    }

    private void fillOutWaitForThreadNode(Config config)
    throws LHValidationError, LHConnectionError {
        NodeSchema sourceNode = threadSpec.nodes.get(threadWaitSourceNodeName);

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
