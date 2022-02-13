package little.horse.common.objects.metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.processor.api.Record;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import little.horse.common.Config;
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
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;
import little.horse.common.util.K8sStuff.Container;
import little.horse.common.util.K8sStuff.Deployment;
import little.horse.common.util.K8sStuff.DeploymentMetadata;
import little.horse.common.util.K8sStuff.DeploymentSpec;
import little.horse.common.util.K8sStuff.EnvEntry;
import little.horse.common.util.K8sStuff.PodSpec;
import little.horse.common.util.K8sStuff.Selector;
import little.horse.common.util.K8sStuff.Template;


// Just scoping for the purposes of the json parser
@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name"
)
public class WFSpec extends CoreMetadata {
    @JsonIgnore
    public static String typeName = "wfSpec";

    // Journalling
    public String name;
    public LHDeployStatus status;
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
    private String kafkaTopic;

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

        wfRun.id = record.key();
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
    public void deploy() throws LHConnectionError {
        config.createKafkaTopic(new NewTopic(this.getKafkaTopic(), getPartitions(),
            getReplicationFactor())
        );
        
        String deployString;

        try {
            deployString = new ObjectMapper(
                new YAMLFactory()).writeValueAsString(getK8sDeployment());
        } catch (JsonProcessingException exn) {
            exn.printStackTrace();
            throw new RuntimeException(
                "Looks like Colt's code created invalid json >_> "
            );
        }

        try {
            Process process = Runtime.getRuntime().exec("kubectl apply -f -");

            process.getOutputStream().write(deployString.getBytes());
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
        } catch(IOException|InterruptedException exn) {
            throw new LHConnectionError(
                exn, "Failed to deploy the wf: " + exn.getMessage()
            );
        }

    }

    @JsonIgnore
    @Override
    public void remove() throws LHConnectionError {
        try {
            Process process = Runtime.getRuntime().exec(
                "kubectl delete deploy -llittlehorse.io/wfSpecId=" + getId()
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
    
            this.status = LHDeployStatus.STOPPED;
        } catch(IOException|InterruptedException exn) {
            throw new LHConnectionError(
                exn, "Failed to deploy the wf: " + exn.getMessage()
            );
        }
        
    }

    @Override
    public void processChange(CoreMetadata old) throws LHConnectionError {
        if (!(old instanceof WFSpec)) {
            throw new RuntimeException(
                "Called processChange on a non-WFSpec"
            );
        }

        WFSpec oldSpec = (WFSpec) old;
        if (oldSpec.status != desiredStatus) {
            if (desiredStatus == LHDeployStatus.RUNNING) {
                deploy();
            } else if (desiredStatus == LHDeployStatus.STOPPED) {
                remove();
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

    public String getKafkaTopic() {
        if (kafkaTopic == null) {
            kafkaTopic = config.getWFRunTopicPrefix() + name + "-"
                + getId().substring(0, 8);
        }
        return kafkaTopic;
    }

    public String getK8sName() {
        if (k8sName == null) {
            k8sName = LHUtil.toValidK8sName(name + "-" + getId().substring(0, 8));
        }
        return k8sName;
    }

    @JsonIgnore
    private Deployment getK8sDeployment() {
        Deployment dp = new Deployment();
        dp.metadata = new DeploymentMetadata();
        dp.spec = new DeploymentSpec();
        dp.kind = "Deployment";
        dp.apiVersion = "apps/v1";

        dp.metadata.name = this.getK8sName();
        dp.metadata.labels = new HashMap<String, String>();
        dp.metadata.namespace = namespace;
        dp.metadata.labels.put("app", this.getK8sName());
        dp.metadata.labels.put("littlehorse.io/wfSpecId", getId());
        dp.metadata.labels.put("littlehorse.io/wfSpecName", name);
        dp.metadata.labels.put("littlehorse.io/active", "true");

        Container container = new Container();
        container.name = this.getK8sName();
        container.image = config.getWfWorkerImage();
        container.imagePullPolicy = "IfNotPresent";
        container.env = config.getBaseK8sEnv();
        container.env.add(new EnvEntry(
            Constants.KAFKA_APPLICATION_ID_KEY,
            getId()
        ));

        container.env.add(new EnvEntry(Constants.WF_SPEC_ID_KEY, getId()));
        container.env.add(new EnvEntry(Constants.NODE_NAME_KEY, name));

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.name = this.getK8sName();
        template.metadata.labels = new HashMap<String, String>();
        template.metadata.namespace = namespace;
        template.metadata.labels.put("app", this.getK8sName());
        template.metadata.labels.put("littlehorse.io/wfSpecId", getId());
        template.metadata.labels.put("littlehorse.io/wfSpecName", name);
        template.metadata.labels.put("littlehorse.io/active", "true");

        template.spec = new PodSpec();
        template.spec.containers = new ArrayList<Container>();
        template.spec.containers.add(container);

        dp.spec.template = template;
        dp.spec.replicas = this.getReplicationFactor();
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", this.getK8sName());
        dp.spec.selector.matchLabels.put("littlehorse.io/wfSpecId", getId());
        dp.spec.selector.matchLabels.put("littlehorse.io/wfSpecName", name);
        dp.spec.selector.matchLabels.put("littlehorse.io/active", "true");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            String result = mapper.writeValueAsString(dp);
            LHUtil.log("Node tok8s: ", result);
        } catch (JsonProcessingException exn) {
            LHUtil.logError(exn.getMessage());
        }

        return dp;
    }
}
