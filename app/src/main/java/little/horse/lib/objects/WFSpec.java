package little.horse.lib.objects;


import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHDeployError;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHLookupExceptionReason;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.NodeType;
import little.horse.lib.K8sStuff.Deployment;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeSchema;
import little.horse.lib.schemas.NodeSchema;

import little.horse.lib.schemas.ThreadSpecSchema;
import little.horse.lib.schemas.VariableAssignmentSchema;
import little.horse.lib.schemas.VariableMutationSchema;
import little.horse.lib.schemas.WFRunVariableDefSchema;
import little.horse.lib.schemas.WFSpecSchema;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;


public class WFSpec {
    private Config config;
    private WFSpecSchema schema;
    private String k8sName;

    /**
     * This is used in the constructor during validation of the WFSpec. It stores
     * a mapping of a thread name to it's variable definitions. During the
     * cleanupThreadSpec() method, this gets populated with the variableDefs for
     * that thread. Then, on the validateVariables() method, this map is consulted
     * to determine whether:
     * 1. There is any conflict in variable names
     * 2. There are any references to variables that aren't defined
     * 3. There are any references to variables that are guaranteed to be out of
     *    scope (i.e. parent thread referencing variable of child thread with no
     *    way to get to the child thread from the parent).
     */
    private HashMap<String, HashMap<String, WFRunVariableDefSchema>> allVarDefs;

    private void cleanupTaskNode(NodeSchema node) throws LHValidationError {
        if (node.taskDefinitionName == null) {
            throw new LHValidationError(
                "Invalid Node " + node.name + ": No taskDefinition supplied"
            );
        }
        try {
            TaskDef.fromIdentifier(node.taskDefinitionName, config);
        } catch (LHLookupException exn) {
            if (exn.getReason() == LHLookupExceptionReason.OBJECT_NOT_FOUND) {
                throw new LHValidationError(
                    "No taskDef named " + node.taskDefinitionName + " found."
                );
            } else {
                throw new LHValidationError(
                    "Failed looking up TaskDef " + node.taskDefinitionName
                );
            }
        }
    }

    private void cleanupExternalEventNode(NodeSchema node) throws LHValidationError {
        ExternalEventDef eed = null;
        if (node.externalEventDefGuid != null) {
            try {
                eed = ExternalEventDef.fromIdentifier(
                    node.externalEventDefGuid, config
                );
            } catch (LHLookupException exn) {
                throw new LHValidationError(
                    "Could not find externaleventdef " + node.externalEventDefGuid
                );
            }
        } else if (node.externalEventDefName != null) {
            try {
                eed = ExternalEventDef.fromIdentifier(
                    node.externalEventDefName, config
                );
            } catch (LHLookupException exn) {
                exn.printStackTrace();
                throw new LHValidationError(
                    "Could not find externaleventdef " + node.externalEventDefName
                );
            }
        }

        node.externalEventDefGuid = eed.getModel().guid;
        node.externalEventDefName = eed.getModel().name;
    }

    private void cleanupSpawnThreadNode(NodeSchema node) throws LHValidationError {
        String tname = node.threadSpawnThreadSpecName;
        if (tname == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + node.name + " specifies no thread to spawn."
            );
        }

        ThreadSpecSchema tspec = schema.threadSpecs.get(tname);
        if (tspec == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + node.name + " specified unknown thread to" +
                " spawn: " + tname
            );
        }
    }

    private void cleanupWaitForThreadNode(NodeSchema node) throws LHValidationError {
        ThreadSpecSchema thread = schema.threadSpecs.get(node.threadSpecName);
        NodeSchema sourceNode = thread.nodes.get(node.threadWaitSourceNodeName);

        if (sourceNode == null) {
            throw new LHValidationError(
                "Wait for thread node " + node.name + "has no valid source node " +
                "from the same thread specified."
            );
        }
        if (sourceNode.nodeType != NodeType.SPAWN_THREAD) {
            throw new LHValidationError(
                "Wait For Thread Node " + node.name + " references a node that is" +
                " not of type SPAWN_THREAD: " + sourceNode.name + ": " +
                sourceNode.nodeType
            );
        }

        // TODO: Throw an error if the WAIT_FOR_THREAD node doesn't come after
        // the SPAWN_THREAD node. Can figure this out by doing analysis on the
        // graph of nodes/edges on the ThreadSpec.

        node.threadWaitSourceNodeGuid = sourceNode.guid;
        node.threadWaitSourceNodeName = sourceNode.name;
    }

    private void cleanupNode(String name, NodeSchema node, ThreadSpecSchema thread)
    throws LHValidationError {
        node.threadSpecName = thread.name;
        node.wfSpecGuid = schema.guid;
        if (node.guid == null) {
            node.guid = LHUtil.generateGuid();
        }
        
        if (node.name == null) {
            node.name = name;
        } else if (!node.name.equals(name)) {
            throw new LHValidationError(
                "Node name didn't match for node " + name
            );
        }
        if (node.variableMutations == null) {
            node.variableMutations = new HashMap<String, VariableMutationSchema>();
        }
        if (node.variables == null) {
            node.variables = new HashMap<String, VariableAssignmentSchema>();
        }
        if (node.outgoingEdges == null) {
            node.outgoingEdges = new ArrayList<EdgeSchema>();
        }
        if (node.incomingEdges == null) {
            node.incomingEdges = new ArrayList<EdgeSchema>();
        }
    }

    private void cleanupEdge(EdgeSchema edge, ThreadSpecSchema thread) {
        edge.wfSpecGuid = schema.guid;
        if (edge.guid == null) {
            edge.guid = LHUtil.generateGuid();
        }
        NodeSchema source = thread.nodes.get(edge.sourceNodeName);
        NodeSchema sink = thread.nodes.get(edge.sinkNodeName);
        edge.sourceNodeGuid = source.guid;
        edge.sinkNodeGuid = sink.guid;

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

    private String calculateEntrypointNode(ThreadSpecSchema thread) throws LHValidationError {
        NodeSchema entrypoint = null;
        for (Map.Entry<String, NodeSchema> pair: thread.nodes.entrySet()) {
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
        return entrypoint.name;
    }

    private void cleanupThreadSpec(ThreadSpecSchema spec, String name) throws LHValidationError {
        spec.name = name;
        
        if (spec.variableDefs == null) {
            spec.variableDefs = new HashMap<String, WFRunVariableDefSchema>();
        }
        
        // Light Clean up the node (i.e. give it a guid if it doesn't have one, etc), validate that
        // its taskdef exist
        for (Map.Entry<String, NodeSchema> pair: spec.nodes.entrySet()) {
            cleanupNode(pair.getKey(), pair.getValue(), spec);
        }
        // Now do the deep clean.
        for (Map.Entry<String, NodeSchema> pair: spec.nodes.entrySet()) {
            NodeSchema node = pair.getValue();
            if (node.nodeType == NodeType.TASK) {
                cleanupTaskNode(node);
            } else if (node.nodeType == NodeType.EXTERNAL_EVENT) {
                cleanupExternalEventNode(node);
            } else if (node.nodeType == NodeType.SPAWN_THREAD) {
                cleanupSpawnThreadNode(node);
            } else if (node.nodeType == NodeType.WAIT_FOR_THREAD) {
                cleanupWaitForThreadNode(node);
            }
        }


        if (spec.edges == null) {
            spec.edges = new ArrayList<EdgeSchema>();
        }
        for (EdgeSchema edge : spec.edges) {
            cleanupEdge(edge, spec);
        }

        // Next, assign the entryoint node
        if (spec.entrypointNodeName == null) {
            spec.entrypointNodeName = calculateEntrypointNode(spec);
        }

        // Now, iterate through variables and add them to the allVarDefs map.
        if (allVarDefs.containsKey(name)) {
            throw new LHValidationError(
                "Thread name " + name + " used twice in the workflow! OrzDash!"
            );
        }
        allVarDefs.put(name, spec.variableDefs);
    }

    /**
     * Validates that:
     * 1. All variables referred to in the WFSpec are defined.
     * 2. There are no references to variables that cannot possibly be in scope.
     * 3. There are no conflicts between names of variables for threads that might
     *    share scope.
     */
    private void validateVariables() throws LHValidationError {
        // First, ensure that no variable names are used twice.
        makeSureNoDuplicateVarNames();

        // Now we want to construct a map from thread name to list of
        // variables that are surely defined at that point.
        HashSet<String> seenThreads = new HashSet<String>();
        HashMap<String, String> visibleVariables = new HashMap<>();

        // This will get tricky with interrupts, but...
        validateVariablesHelper(
            seenThreads,
            visibleVariables,
            schema.entrypointThreadName
        );
        
    }

    /**
     * Pardon my recursion.
     * @param seenThreads Set of all threads we've already visited (to avoid circular
     * processing)
     * @param allVisibleVariables think of this as a set of all variables we've seen
     * so far. The key is the variable name, and the value is the name of the thread
     * it came from (so that we can raise an error with a descriptive message
     * saying which two threadspecs are conflicting with each other.)
     * @param threadName the name of the thread we do be processin' rn.
     */
    private void validateVariablesHelper(
        HashSet<String> seenThreads,
        HashMap<String, String> seenVars,
        String threadName
    ) throws LHValidationError{
        if (seenThreads.contains(threadName)) {
            return;
        }

        ThreadSpecSchema thread = schema.threadSpecs.get(threadName);
        for (String varName: thread.variableDefs.keySet()) {
            if (seenVars.containsKey(varName)) {
                throw new LHValidationError(
                    "Variable " + varName + " defined again in child thread " +
                    threadName + " after being defined in thread " +
                    seenVars.get(varName)
                );
            }
            seenVars.put(varName, threadName);
        }

        // Now iterate through all of the tasks in this thread and see if the
        // variables are defined.
        for (NodeSchema node: thread.nodes.values()) {
            for (String varName: node.variables.keySet()) {
                VariableAssignmentSchema assign = node.variables.get(varName);
                if (!seenVars.containsKey(assign.wfRunVariableName)) {
                    throw new LHValidationError(
                        "Variable " + varName + "refers to wfRunVariable named " +
                        assign.wfRunVariableName + ", which is either not defined"
                        + " or not in scope for thread " + thread.name + " on node "
                        + node.name
                    );
                }
            }

            for (String varName: node.variableMutations.keySet()) {
                if (!seenVars.containsKey(varName)) {
                    throw new LHValidationError(
                        "Variable " + varName + " either not defined or not in " +
                        "scope for thread " + thread.name + " on node " + node.name
                    );
                }
            }
        }

        // Now process every potential child thread.
        for (NodeSchema node: thread.nodes.values()) {
            if (node.nodeType == NodeType.SPAWN_THREAD) {
                String nextThreadName = node.threadSpawnThreadSpecName;
                validateVariablesHelper(seenThreads, seenVars, nextThreadName);
            }
        }

        // Due to recursive backtracking, we need to remove the elements we added.
        for (String varName: thread.variableDefs.keySet()) {
            seenVars.remove(varName);
        }
        seenThreads.remove(threadName);
    }

    private void makeSureNoDuplicateVarNames() throws LHValidationError {
        HashSet<String> seen = new HashSet<String>();
        for (Map.Entry<
                String, HashMap<String, WFRunVariableDefSchema>
            > e: allVarDefs.entrySet()
        ) {
            for (String varName: e.getValue().keySet()) {
                if (seen.contains(varName)) {
                    throw new LHValidationError(
                        "Variable " + varName + " defined twice! No bueno."
                    );
                }
                seen.add(varName);
            }
        }
    }

    public WFSpec(WFSpecSchema schema, Config config) throws LHValidationError {
        this.config = config;
        this.schema = schema;
        // TODO (hard): do some validation that we don't have a 409.
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.kafkaTopic == null) {
            schema.kafkaTopic = config.getWFRunTopicPrefix() + schema.name + "_" + schema.guid;
        }

        if (schema.status == null) {
            schema.status = LHStatus.STOPPED;
        }

        if (schema.desiredStatus == null) {
            schema.desiredStatus = LHStatus.RUNNING;
        }

        allVarDefs = new HashMap<String, HashMap<String, WFRunVariableDefSchema>>();

        for (Map.Entry<String, ThreadSpecSchema> pair: schema.threadSpecs.entrySet()) {
            cleanupThreadSpec(pair.getValue(), pair.getKey());
        }

        validateVariables();

        this.k8sName = LHUtil.toValidK8sName(
            this.schema.name + "-" + LHUtil.digestify(schema.guid)
        );
    }

    public WFSpecSchema getModel() {
        return this.schema;
    }

    public String getNamespace() {
        return "default";
    }

    public String toString() {
        return schema.toString();
    }

    public String getK8sName() {
        return this.k8sName;
    }

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getWFSpecTopic(),
            schema.guid,
            this.toString()
        );
        this.config.send(record);
    }

    public ArrayList<Node> getNodes() {
        ArrayList<Node> list = new ArrayList<Node>();
        for (Map.Entry<String, ThreadSpecSchema> t: schema.threadSpecs.entrySet()) {
            ThreadSpecSchema tspec = t.getValue();
            for (Map.Entry<String, NodeSchema> n : tspec.nodes.entrySet()) {
                try {
                    list.add(new Node(n.getValue(), this, config));
                } catch (LHLookupException exn) {
                    LHUtil.logError(
                        "Shouldn't be possible to have this but we do.",
                        exn.getMessage()
                    );
                    // Nothing to do, because shouldn't be able to get here.
                }
            }
        }
        return list;
    }

    public int getPartitions() {
        return config.getDefaultPartitions();
    }

    public short getReplicationFactor() {
        return (short) config.getDefaultReplicas();
    }

    public void deploy() throws LHDeployError {
        // First, create the kafka topics
        config.createKafkaTopic(new NewTopic(
            this.schema.kafkaTopic, getPartitions(), getReplicationFactor()
        ));

        ArrayList<String> ymlStrings = new ArrayList<String>();
        // Finally, deploy task daemons for each of the Node's in the workflow.
        for (Node node : this.getNodes()) {
            try {
                Deployment dp = node.getK8sDeployment();
                if (dp != null) {
                    ymlStrings.add(new ObjectMapper(new YAMLFactory()).writeValueAsString(dp));
                }
            } catch (JsonProcessingException exn) {
                exn.printStackTrace();
                throw new LHDeployError("Had an orzdash");
            }
        }

        for (String yml : ymlStrings) {
            try {
                LHUtil.log("About to apply this: ", yml, "\n\n");
                Process process = Runtime.getRuntime().exec("kubectl apply -f -");

                process.getOutputStream().write(yml.getBytes());
                process.getOutputStream().close();
                process.waitFor();

                BufferedReader input = new BufferedReader(
                    new InputStreamReader(process.getInputStream())
                );
                String line = null;
                while ((line = input.readLine()) != null) {
                    LHUtil.log(line);
                }

                BufferedReader error = new BufferedReader(
                    new InputStreamReader(process.getErrorStream())
                );
                line = null;
                while ((line = error.readLine()) != null) {
                    LHUtil.log(line);
                }

            } catch (Exception exn) {
                exn.printStackTrace();
                throw new LHDeployError("had an orzdash");
            }
        }
        this.schema.status = LHStatus.RUNNING;
        this.record();
    }

    public void undeploy() {
        try {
            Process process = Runtime.getRuntime().exec(
                "kubectl delete deploy -llittlehorse.io/wfSpecGuid=" + this.schema.guid
            );
            process.getOutputStream().close();
            process.waitFor();
            BufferedReader input = new BufferedReader(
                new InputStreamReader(process.getInputStream())
            );
            String line = null;
            while ((line = input.readLine()) != null) {
                LHUtil.log(line);
            }

            BufferedReader error = new BufferedReader(
                new InputStreamReader(process.getErrorStream())
            );
            line = null;
            while ((line = error.readLine()) != null) {
                LHUtil.log(line);
            }

            this.schema.status = LHStatus.REMOVED;
        } catch (Exception exn) {
            exn.printStackTrace();
            this.schema.status = LHStatus.ERROR;
        }

        this.record();
    }

}
