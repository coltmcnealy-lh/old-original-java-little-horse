package little.horse.common.objects.metadata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.api.Record;

import little.horse.common.Config;
import little.horse.common.events.ExternalEventCorrelSchema;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFRunRequestSchema;
import little.horse.common.exceptions.LHDeployError;
import little.horse.common.exceptions.LHLookupException;
import little.horse.common.exceptions.LHNoConfigException;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHStatus;
import little.horse.common.objects.rundata.ThreadRunMetaSchema;
import little.horse.common.objects.rundata.ThreadRunSchema;
import little.horse.common.objects.rundata.WFRunSchema;
import little.horse.common.objects.rundata.WFRunStatus;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;
import little.horse.common.util.K8sStuff.Deployment;

public class WFSpecSchema extends BaseSchema {
    public String name;
    public String guid;
    public LHStatus status;
    public String kafkaTopic;
    public String entrypointThreadName;
    public LHStatus desiredStatus;
    public String k8sName;
    public String namespace;

    public HashMap<String, ThreadSpecSchema> threadSpecs;
    public HashSet<String> interruptEvents;

    // All fields below ignored by Json
    @JsonIgnore
    private HashMap<String, HashMap<String, WFRunVariableDefSchema>> allVarDefs;

    /**
     * Called by the API when creating a new WFSpec. Infers some fields and ensures
     * that we have a valid spec. It checks that variable definitions and references
     * are kosher, all task/externalEvent definitions exist, the edges in the spec
     * make sense, etc.
     * @param config is a little.horse.lib.Config.
     * @throws LHValidationError if there's an invalid spec.
     */
    public void cleanupAndValidate(Config config) throws LHValidationError {
        setConfig(config);

        if (guid == null) guid = LHUtil.generateGuid();
        if (kafkaTopic == null) {
            kafkaTopic = config.getWFRunTopicPrefix() + name + "-" + guid;
        }
        if (status == null) status = LHStatus.STOPPED;
        if (desiredStatus == null) desiredStatus = LHStatus.RUNNING;
        allVarDefs = new HashMap<String, HashMap<String, WFRunVariableDefSchema>>();
        if (namespace == null) namespace = "default";

        for (Map.Entry<String, ThreadSpecSchema> pair: threadSpecs.entrySet()) {
            cleanupThreadSpec(pair.getValue(), pair.getKey());
        }
        validateVariables();
        k8sName = LHUtil.toValidK8sName(name + "-" + LHUtil.digestify(guid));
    }

    private void cleanupThreadSpec(ThreadSpecSchema spec, String name) throws LHValidationError {
        spec.name = name;
        
        if (spec.variableDefs == null) {
            spec.variableDefs = new HashMap<String, WFRunVariableDefSchema>();
        }
        if (spec.interruptDefs == null) {
            spec.interruptDefs = new HashMap<String, InterruptDefSchema>();
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

        // validate interrupt handlers.
        // TODO: More fancy validation where we make sure there's only one input
        // variable.
        interruptEvents = new HashSet<String>();
        for (Map.Entry<String, InterruptDefSchema> p: spec.interruptDefs.entrySet()) {
            interruptEvents.add(p.getKey()); // know to handle this event as interrupt
            String tspecName = p.getValue().threadSpecName;
            if (!threadSpecs.containsKey(tspecName)) {
                throw new LHValidationError(
                    "Interrupt handler " + p.getKey() + " references nonexistent" +
                    "thread named " + tspecName
                );
            }
        }

        if (spec.edges == null) {
            spec.edges = new ArrayList<EdgeSchema>();
        }
        for (EdgeSchema edge : spec.edges) {
            cleanupEdge(edge, spec);
        }

        spec.entrypointNodeName = calculateEntrypointNode(spec);

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
            this.entrypointThreadName
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

        ThreadSpecSchema thread = this.threadSpecs.get(threadName);
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
                if (assign.wfRunVariableName == null) {
                    continue;
                }
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
            if (node.baseExceptionhandler != null) {
                String threadSpec = node.baseExceptionhandler.handlerThreadSpecName;
                if (threadSpec != null) {
                    validateVariablesHelper(seenThreads, seenVars, threadSpec);
                }
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

    private void cleanupTaskNode(NodeSchema node) throws LHValidationError {
        if (node.taskDef == null) {
            LHUtil.log(node);
            throw new LHValidationError(
                "Node " + node.name + " is type TASK but provides no TaskDef"
            );
        }
        node.taskDef.validateAndCleanup(config);
    }

    private void cleanupEdge(EdgeSchema edge, ThreadSpecSchema thread) {
        edge.wfSpecGuid = guid;
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

    @JsonIgnore
    private String calculateEntrypointNode(ThreadSpecSchema thread) throws LHValidationError {
        if (thread.entrypointNodeName != null) {
            return thread.entrypointNodeName;
        }
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
        if (entrypoint == null) {
            throw new LHValidationError(
                "No entrypoint specified and no node present without incoming edges."
            );
        }
        return entrypoint.name;
    }

    private void cleanupNode(String name, NodeSchema node, ThreadSpecSchema thread)
    throws LHValidationError {
        node.threadSpecName = thread.name;
        node.wfSpecGuid = guid;
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

        if (node.baseExceptionhandler != null) {
            String handlerSpecName = node.baseExceptionhandler.handlerThreadSpecName;
            if (handlerSpecName != null) {
                ThreadSpecSchema handlerSpec = threadSpecs.get(handlerSpecName);
                if (handlerSpec == null) {
                    throw new LHValidationError(
                        "Exception handler on node " + node.name + " refers to " +
                        "thread spec that doesn't exist: " + handlerSpecName
                    );
                }
                // TODO: Maybe we should enforce "the exception handler thread
                // can't have input variables"
            }
        }
    }

    private void cleanupExternalEventNode(NodeSchema node) throws LHValidationError {
        ExternalEventDefSchema eed = null;
        String id = node.externalEventDefGuid == null ? node.externalEventDefName :
            node.externalEventDefGuid;
        try {
            eed = LHDatabaseClient.lookupExternalEventDef(id, config);
        } catch (LHLookupException exn) {
            throw new LHValidationError(
                "Could not find externaleventdef " + node.externalEventDefGuid +
                " / " + node.externalEventDefName + "\n" + exn.getMessage()
            );
        }

        node.externalEventDefGuid = eed.guid;
        node.externalEventDefName = eed.name;
    }

    private void cleanupSpawnThreadNode(NodeSchema node) throws LHValidationError {
        String tname = node.threadSpawnThreadSpecName;
        if (tname == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + node.name + " specifies no thread to spawn."
            );
        }

        ThreadSpecSchema tspec = this.threadSpecs.get(tname);
        if (tspec == null) {
            throw new LHValidationError(
                "Thread Spawn Node " + node.name + " specified unknown thread to" +
                " spawn: " + tname
            );
        }
    }

    private void cleanupWaitForThreadNode(NodeSchema node) throws LHValidationError {
        ThreadSpecSchema thread = this.threadSpecs.get(node.threadSpecName);
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

    @JsonIgnore
    public ArrayList<Map.Entry<String, NodeSchema>> allNodePairs() {
        ArrayList<Map.Entry<String, NodeSchema>> out = new ArrayList<>();
        for (Map.Entry<String, ThreadSpecSchema> tp: threadSpecs.entrySet()) {
            ThreadSpecSchema t = tp.getValue();
            for (Map.Entry<String, NodeSchema> np: t.nodes.entrySet()) {
                out.add(np);
            }
        }
        return out;
    }

    @JsonIgnore
    public WFRunSchema newRun(
        final Record<String, WFEventSchema> record
    ) throws LHNoConfigException, LHLookupException {
        WFRunSchema wfRun = new WFRunSchema();
        WFEventSchema event = record.value();
        WFRunRequestSchema runRequest = BaseSchema.fromString(
            event.content, WFRunRequestSchema.class
        );

        wfRun.guid = record.key();
        wfRun.wfSpecGuid = event.wfSpecGuid;
        wfRun.wfSpecName = event.wfSpecName;
        wfRun.setWFSpec(this);

        wfRun.status = WFRunStatus.RUNNING;
        wfRun.threadRuns = new ArrayList<ThreadRunSchema>();
        wfRun.correlatedEvents =
            new HashMap<String, ArrayList<ExternalEventCorrelSchema>>();

        wfRun.startTime = event.timestamp;
        wfRun.awaitableThreads = new HashMap<String, ArrayList<ThreadRunMetaSchema>>();

        wfRun.threadRuns.add(wfRun.createThreadClientAdds(
            entrypointThreadName, runRequest.variables, null
        ));

        return wfRun;
    }

    @JsonIgnore
    public int getPartitions() {
        return config.getDefaultPartitions();
    }

    @JsonIgnore
    public short getReplicationFactor() {
        return (short) config.getDefaultReplicas();
    }

    public void deploy() throws LHDeployError {
        config.createKafkaTopic(new NewTopic(kafkaTopic, getPartitions(),
            getReplicationFactor()));
        
        ArrayList<String> ymlStrings = new ArrayList<String>();

        for (ThreadSpecSchema tspec: threadSpecs.values()) {
            for (NodeSchema node: tspec.nodes.values()) {
                try {
                    Deployment dp = node.getK8sDeployment();
                    if (dp != null) {
                        ymlStrings.add(new ObjectMapper(
                            new YAMLFactory()).writeValueAsString(dp));
                    }
                } catch (JsonProcessingException exn) {
                    exn.printStackTrace();
                    throw new LHDeployError("Had an orzdash");
                }
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
        status = LHStatus.RUNNING;
        record();
    }

    public void undeploy() {
        try {
            Process process = Runtime.getRuntime().exec(
                "kubectl delete deploy -llittlehorse.io/wfSpecGuid=" + guid
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

            this.status = LHStatus.REMOVED;
        } catch (Exception exn) {
            exn.printStackTrace();
            this.status = LHStatus.ERROR;
        }

        this.record();
    }

    @JsonIgnore
    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getWFSpecTopic(), guid, this.toString());
        this.config.send(record);
    }

    @JsonIgnore
    public Config setConfig(Config config) {
        super.setConfig(config);
        for (ThreadSpecSchema threadSpec: threadSpecs.values()) {
            threadSpec.setConfig(config);
            if (threadSpec.wfSpec == null) {
                threadSpec.wfSpec = this;
            }
        }
        
        return this.config;
    }
}
