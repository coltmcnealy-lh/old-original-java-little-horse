package little.horse.common.objects.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import little.horse.common.DepInjContext;
import little.horse.common.events.ExternalEventCorrel;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFRunRequest;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.DigestIgnore;
import little.horse.common.objects.rundata.ThreadRunMeta;
import little.horse.common.objects.rundata.ThreadRun;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.objects.rundata.LHExecutionStatus;
import little.horse.common.util.LHUtil;
import little.horse.lib.deployers.WorkflowDeployer;


// Just scoping for the purposes of the json parser
@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name"
)
public class WFSpec extends CoreMetadata {
    @JsonIgnore
    public static String typeName = "wfSpec";

    // Journalling
    @DigestIgnore
    public LHDeployStatus status;
    @DigestIgnore
    public LHDeployStatus desiredStatus;

    // Actual definition here.
    @JsonManagedReference
    public HashMap<String, ThreadSpec> threadSpecs;
    public HashSet<String> interruptEvents;
    public String entrypointThreadName;

    // Stuff for deployment info
    @JsonIgnore // see getter()
    private String k8sName;
    public String namespace;
    @JsonIgnore // see getter()
    private String eventTopic;
    @JsonIgnore // see Getter
    private String timerTopic;

    private String wfDeployerClassName;
    public String getWfDeployerClassName() {
        if (wfDeployerClassName == null) {
            wfDeployerClassName = config.getDefaultWFDeployerClassName();
        }
        return wfDeployerClassName;
    }
    public void setWfDeployerClassName(String name) {
        this.wfDeployerClassName = name;
    }

    @JsonIgnore
    public WorkflowDeployer getWFDeployer() {
        return LHUtil.loadClass(getWfDeployerClassName());
    }

    public String deployMetadata;

    // Internal bookkeeping for validation
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

            if (node.sleepSeconds != null) {
                VariableAssignment assn = node.sleepSeconds;
                if (assn.wfRunVariableName != null) {
                    if (!seenVars.containsKey(node.sleepSeconds.wfRunVariableName)) {
                        throw new LHValidationError(
                            "refers to wfRunVariable named " +
                            assn.wfRunVariableName + ", which is either not defined"
                            + " or not in scope for thread " + thread.name +
                            " on node " + node.name
                        );
                    }
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
    public WFRun newRun(String id, WFEvent event) throws LHConnectionError {
        WFRun wfRun = new WFRun();
        WFRunRequest runRequest;
        try {
            runRequest = BaseSchema.fromString(
                event.content, WFRunRequest.class, config
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            throw new RuntimeException("TODO: Handle when the runRequest is invalid");
        }

        wfRun.id = id;
        wfRun.wfSpecDigest = event.wfSpecId;
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
    public void deploy(boolean shouldCreateTopics) throws LHConnectionError {
        if (shouldCreateTopics) {
            config.createKafkaTopic(new NewTopic(this.getEventTopic(), getPartitions(),
                getReplicationFactor())
            );

            config.createKafkaTopic(new NewTopic(
                this.getTimerTopic(),getPartitions(), getReplicationFactor()
            ));
        }

        getWFDeployer().deploy(this, config);
    }

    @JsonIgnore
    @Override
    public void remove() throws LHConnectionError {
        getWFDeployer().undeploy(this, config);        
    }

    @Override
    public void processChange(CoreMetadata old) throws LHConnectionError {
        if (old != null && !(old instanceof WFSpec)) {
            throw new RuntimeException(
                "Called processChange on a non-WFSpec"
            );
        }

        WFSpec oldSpec = (WFSpec) old;
        if (oldSpec == null || oldSpec.status != desiredStatus) {
            if (desiredStatus == LHDeployStatus.RUNNING) {
                boolean shouldCreateTopic = (oldSpec == null);
                deploy(shouldCreateTopic);
            } else if (desiredStatus == LHDeployStatus.STOPPED) {
                remove();
            }
        }
    }

    @Override
    public void validate(DepInjContext config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        if (status == null) status = LHDeployStatus.STOPPED;
        if (desiredStatus == null) desiredStatus = LHDeployStatus.RUNNING;
        if (namespace == null) namespace = "default";  // Trololol
        if (allVarDefs == null) allVarDefs = new HashMap<>();

        // TODO: This doesn't yet support lookUpOrCreateExternalEvent.
        // In the future, we'll want to support that use case.
        interruptEvents = new HashSet<String>();
        for (ThreadSpec thread: this.threadSpecs.values()) {
            thread.validate(config, this);

            if (allVarDefs.containsKey(thread.name)) {
                throw new LHValidationError(
                    "Thread name " + thread.name + " used twice in the workflow! OrzDash!"
                );
            }
            allVarDefs.put(thread.name, thread.variableDefs);

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

        validateVariables();
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

    public String getEventTopic() {
        if (eventTopic == null) {
            eventTopic = config.getWFRunEventTopicPrefix() + name + "-"
                + getId().substring(0, 8);
        }
        return eventTopic;
    }

    public String getTimerTopic() {
        if (timerTopic == null) {
            timerTopic = config.getWFRunTimerTopicPrefix() + name + "-"
                + getId().substring(0, 8);
        }
        return timerTopic;
    }

    public void setEventTopic(String foo) {} // just jackson again
    public void setTimerTopic(String foo) {} // just jackson again

    public String getK8sName() {
        if (k8sName == null) {
            k8sName = LHUtil.toValidK8sName(name + "-" + getId().substring(0, 8));
        }
        return k8sName;
    }
    public void setK8sName(String foo) {} //just for the Jackson thing.

    @JsonIgnore
    private HashSet<TaskDef> tqs;

    @JsonIgnore
    public HashSet<TaskDef> getAllTaskDefs() throws LHConnectionError {
        if (tqs != null) return tqs;
        tqs = new HashSet<>();

        for (ThreadSpec t: threadSpecs.values()) {
            for (Node n: t.nodes.values()) {
                if (n.nodeType == NodeType.TASK) {
                    tqs.add(n.taskDef);
                }
            }
        }
        return tqs;
    }
}
