package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.ExecutionError;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.processor.api.Record;

import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIPostResult;
import little.horse.common.Config;
import little.horse.common.events.ExternalEventCorrelSchema;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFRunRequestSchema;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.LHSerdeError;
import little.horse.common.objects.rundata.LHStatus;
import little.horse.common.objects.rundata.ThreadRunMetaSchema;
import little.horse.common.objects.rundata.ThreadRunSchema;
import little.horse.common.objects.rundata.WFRunSchema;
import little.horse.common.objects.rundata.WFRunStatus;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;

public class WFSpecSchema extends CoreMetadata {
    public String name;
    private String guid;
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
    ) throws LHConnectionError {
        WFRunSchema wfRun = new WFRunSchema();
        WFEventSchema event = record.value();
        WFRunRequestSchema runRequest;
        try {
            runRequest = BaseSchema.fromString(
                event.content, WFRunRequestSchema.class, config, true
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            throw new RuntimeException("TODO: Handle when the runRequest is invalid");
        }

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

    @JsonIgnore
    public void deploy() {
        throw new RuntimeException("Implement me!");
    }

    @JsonIgnore
    public void undeploy() {
        throw new RuntimeException("Implement me!");
    }

    @JsonIgnore
    public void remove() {
        throw new RuntimeException("Implement me!");
    }

    @Override
    public void processChange(CoreMetadata old) {
        if (!(old instanceof WFSpecSchema)) {
            throw new RuntimeException(
                "Called processChange on a non-WFSpecSchema."
            );
        }

        WFSpecSchema oldSpec = (WFSpecSchema) old;
        if (oldSpec.status != desiredStatus) {
            if (desiredStatus == LHStatus.RUNNING) {
                deploy();
            } else if (desiredStatus == LHStatus.REMOVED) {
                remove();
            } else if (desiredStatus == LHStatus.STOPPED) {
                undeploy();
            }
        }
    }

    @Override
    public void fillOut(Config config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        if (status == null) status = LHStatus.STOPPED;
        if (desiredStatus == null) desiredStatus = LHStatus.RUNNING;
        if (namespace == null) namespace = "default";  // Trololol

        // TODO: This doesn't yet support lookUpOrCreateExternalEvent.
        // In the future, we'll want to support that use case.
        interruptEvents = new HashSet<String>();
        for (ThreadSpecSchema thread: this.threadSpecs.values()) {
            thread.fillOut(config, this);

            if (allVarDefs.containsKey(name)) {
                throw new LHValidationError(
                    "Thread name " + name + " used twice in the workflow! OrzDash!"
                );
            }
            allVarDefs.put(name, thread.variableDefs);

            for (Map.Entry<String, InterruptDefSchema> p:
                thread.interruptDefs.entrySet()
            ) {
                // know to handle this event as interrupt
                interruptEvents.add(p.getKey());
                String tspecName = p.getValue().threadSpecName;
                if (!threadSpecs.containsKey(tspecName)) {
                    throw new LHValidationError(
                        "Interrupt handler " + p.getKey() + " references nonexistent"
                        + "thread named " + tspecName
                    );
                }
            }
        }
        // No leaf-level CoreMetadata's here, so nothing else to do.

        validateVariables();
        
        guid = getDigest();
        if (kafkaTopic == null) {
            kafkaTopic = config.getWFRunTopicPrefix() + name + "-"
                + guid.substring(0, 8);
        }
        k8sName = LHUtil.toValidK8sName(name + "-" + LHUtil.digestify(guid));
    }

    @JsonIgnore
    public HashSet<NodeSchema> getAllNodes() {
        HashSet<NodeSchema> out = new HashSet<NodeSchema>();
        for (ThreadSpecSchema thread: threadSpecs.values()) {
            for (NodeSchema node: thread.nodes.values()) {
                out.add(node);
            }
        }
        return out;
    }

    @Override
    @JsonIgnore
    @SuppressWarnings("unchecked")  // TODO: Figure out how to not have to do this.
    public LHAPIPostResult<WFSpecSchema> createIfNotExists(APIStreamsContext ctx)
    throws LHConnectionError, LHValidationError {
        LHAPIPostResult<WFSpecSchema> out = new LHAPIPostResult<WFSpecSchema>();

        // First, see if the thing already exists.
        WFSpecSchema old = LHDatabaseClient.lookupMeta(
            getDigest(), config, WFSpecSchema.class
        );
        if (old != null) {
            out.spec = old;
            out.record = null;
            out.name = old.name;
            out.guid = old.getDigest();
            out.status = old.status;
            return out;
        }

        // Ok, so the WFSpec doesn't already exist; now we have to go make it
        // and block until we know whether it was created or it orzdashed.

        // There's a bunch of TaskDef's here, let's create those if they don't exist.
        for (NodeSchema node: getAllNodes()) {
            // We know that node.fillOut() will have already been called. This means
            // that the node's taskDefGuid, taskDefName, and taskDef are all set.
            // But we don't yet know if the node's taskDef was created yet.
            // In the FUTURE, we MIGHT add some optimization to that. But for now,
            // we have to throw the taskDef at the API and see what sticks.

            // TODO: parallelize this.
            LHAPIPostResult<TaskDefSchema> result = node.taskDef.createIfNotExists(
                ctx
            );

            // TODO: Clean this error handling up.
            if (result.message != null) {
                throw new LHValidationError(
                    "Was unable to create taskDef " + node.taskDefName + ": " + 
                    result.message
                );
            }
            if (result.spec == null || !result.spec.isEqualTo(node.taskDef)) {
                throw new LHValidationError(
                    "The taskDef " + node.taskDefName + " was not able to be created."
                );
            }
        }

        // TODO: Create externalEventDef's as well.

        // At this point, we've created all of the TaskDefs/ExternalEventDefs that our
        // WFSpec needs. Now we need to create the actual WFSpec.
        Future<RecordMetadata> fut = record();
        RecordMetadata meta;
        try {
            meta = fut.get();
        } catch (Exception exn) {
            exn.printStackTrace();
            LHUtil.logError("need to figure out something smart to do");
            throw new RuntimeException("orzdash");
        }

        ctx.waitForProcessing(meta, WFSpecSchema.class);

        old = LHDatabaseClient.lookupMeta(
            getDigest(), config, WFSpecSchema.class
        );
        if (old != null) {
            out.spec = old;
            out.record = null;
            out.name = old.name;
            out.guid = old.getDigest();
            out.status = old.status;
            return out;
        }
        throw new RuntimeException("Argh, wtf?");
    }

    
}
