package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.processor.api.Record;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIPostResult;
import little.horse.api.util.LHDeployException;
import little.horse.common.Config;
import little.horse.common.events.ExternalEventCorrel;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFRunRequest;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.DigestIgnore;
import little.horse.common.objects.LHSerdeError;
import little.horse.common.objects.rundata.LHDeployStatus;
import little.horse.common.objects.rundata.ThreadRunMeta;
import little.horse.common.objects.rundata.ThreadRun;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.objects.rundata.LHExecutionStatus;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;


// Just scoping for the purposes of the json parser
@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name"
)
public class WFSpec extends CoreMetadata {
    public static String typeName = "wfSpec";

    public String name;
    public String myDigest;
    public LHDeployStatus status;
    public String kafkaTopic;
    public String entrypointThreadName;
    public LHDeployStatus desiredStatus;
    public String k8sName;
    public String namespace;
    public HashSet<String> interruptEvents;

    @JsonManagedReference
    public HashMap<String, ThreadSpec> threadSpecs;

    @JsonIgnore
    @DigestIgnore
    private HashMap<String, HashMap<String, WFRunVariableDef>> allVarDefs;

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

        ThreadSpec thread = this.threadSpecs.get(threadName);
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
        for (Node node: thread.nodes.values()) {
            for (String varName: node.variables.keySet()) {
                VariableAssignment assign = node.variables.get(varName);
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
        for (Node node: thread.nodes.values()) {
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
                String, HashMap<String, WFRunVariableDef>
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
    public ArrayList<Map.Entry<String, Node>> allNodePairs() {
        ArrayList<Map.Entry<String, Node>> out = new ArrayList<>();
        for (Map.Entry<String, ThreadSpec> tp: threadSpecs.entrySet()) {
            ThreadSpec t = tp.getValue();
            for (Map.Entry<String, Node> np: t.nodes.entrySet()) {
                out.add(np);
            }
        }
        return out;
    }

    @JsonIgnore
    public WFRun newRun(
        final Record<String, WFEvent> record
    ) throws LHConnectionError {
        WFRun wfRun = new WFRun();
        WFEvent event = record.value();
        WFRunRequest runRequest;
        try {
            runRequest = BaseSchema.fromString(
                event.content, WFRunRequest.class, config
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            throw new RuntimeException("TODO: Handle when the runRequest is invalid");
        }

        wfRun.guid = record.key();
        wfRun.wfSpecDigest = event.wfSpecDigest;
        wfRun.wfSpecName = event.wfSpecName;
        wfRun.setWFSpec(this);

        wfRun.status = LHExecutionStatus.RUNNING;
        wfRun.threadRuns = new ArrayList<ThreadRun>();
        wfRun.correlatedEvents =
            new HashMap<String, ArrayList<ExternalEventCorrel>>();

        wfRun.startTime = event.timestamp;
        wfRun.awaitableThreads = new HashMap<String, ArrayList<ThreadRunMeta>>();

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
    public void undeploy() {
        throw new RuntimeException("Implement me!");
    }

    @JsonIgnore
    public void remove() {
        throw new RuntimeException("Implement me!");
    }

    @Override
    public void processChange(CoreMetadata old) {
        if (!(old instanceof WFSpec)) {
            throw new RuntimeException(
                "Called processChange on a non-WFSpecSchema."
            );
        }

        WFSpec oldSpec = (WFSpec) old;
        if (oldSpec.status != desiredStatus) {
            if (desiredStatus == LHDeployStatus.RUNNING) {
                deploy();
            } else if (desiredStatus == LHDeployStatus.REMOVED) {
                remove();
            } else if (desiredStatus == LHDeployStatus.STOPPED) {
                undeploy();
            }
        }
    }

    @Override
    public void validate(Config config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        if (status == null) status = LHDeployStatus.STOPPED;
        if (desiredStatus == null) desiredStatus = LHDeployStatus.RUNNING;
        if (namespace == null) namespace = "default";  // Trololol

        // TODO: This doesn't yet support lookUpOrCreateExternalEvent.
        // In the future, we'll want to support that use case.
        interruptEvents = new HashSet<String>();
        for (ThreadSpec thread: this.threadSpecs.values()) {
            thread.fillOut(config, this);

            if (allVarDefs.containsKey(name)) {
                throw new LHValidationError(
                    "Thread name " + name + " used twice in the workflow! OrzDash!"
                );
            }
            allVarDefs.put(name, thread.variableDefs);

            for (Map.Entry<String, InterruptDef> p:
                thread.interruptDefs.entrySet()
            ) {
                // know to handle this event as interrupt
                interruptEvents.add(p.getKey());
                String tspecName = p.getValue().handlerThreadName;
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
        
        myDigest = getDigest();
        if (kafkaTopic == null) {
            kafkaTopic = config.getWFRunTopicPrefix() + name + "-"
                + myDigest.substring(0, 8);
        }
        k8sName = LHUtil.toValidK8sName(name + "-" + LHUtil.digestify(myDigest));
    }

    @JsonIgnore
    public HashSet<Node> getAllNodes() {
        HashSet<Node> out = new HashSet<Node>();
        for (ThreadSpec thread: threadSpecs.values()) {
            for (Node node: thread.nodes.values()) {
                out.add(node);
            }
        }
        return out;
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")  // TODO: Figure out how to not have to do this.
    public LHAPIPostResult<WFSpec> createIfNotExists(APIStreamsContext ctx)
    throws LHDeployException {
        try {
            return createHelper(ctx);
        } catch (LHValidationError exn) {
            throw new LHDeployException(exn, exn.getMessage(), 400);
        } catch (LHConnectionError exn) {
            throw new LHDeployException(exn, exn.getMessage(), 500);
        }
    }

    @JsonIgnore
    private LHAPIPostResult<WFSpec> createHelper(APIStreamsContext ctx)
    throws LHValidationError, LHConnectionError, LHDeployException {
        LHAPIPostResult<WFSpec> out = new LHAPIPostResult<WFSpec>();

        // First, see if the thing already exists.
        WFSpec old = LHDatabaseClient.lookupMeta(
            getDigest(), config, WFSpec.class
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
        for (Node node: getAllNodes()) {
            // We know that node.fillOut() will have already been called. This means
            // that the node's taskDefGuid, taskDefName, and taskDef are all set.
            // But we don't yet know if the node's taskDef was created yet.
            // In the FUTURE, we MIGHT add some optimization to that. But for now,
            // we have to throw the taskDef at the API and see what sticks.

            // TODO: parallelize this.
            LHAPIPostResult<TaskDef> result = node.taskDef.createIfNotExists(
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

        ctx.waitForProcessing(meta, WFSpec.class);

        old = LHDatabaseClient.lookupMeta(
            getDigest(), config, WFSpec.class
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