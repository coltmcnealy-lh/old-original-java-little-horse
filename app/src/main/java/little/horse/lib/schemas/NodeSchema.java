package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import little.horse.lib.Constants;
import little.horse.lib.LHDatabaseClient;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHUtil;
import little.horse.lib.NodeType;
import little.horse.lib.K8sStuff.Container;
import little.horse.lib.K8sStuff.Deployment;
import little.horse.lib.K8sStuff.DeploymentMetadata;
import little.horse.lib.K8sStuff.DeploymentSpec;
import little.horse.lib.K8sStuff.EnvEntry;
import little.horse.lib.K8sStuff.PodSpec;
import little.horse.lib.K8sStuff.Selector;
import little.horse.lib.K8sStuff.Template;
import little.horse.lib.schemas.NodeSchema;

public class NodeSchema extends BaseSchema {
    public String name;
    public NodeType nodeType;
    public String taskDefinitionName;
    public String wfSpecGuid;
    public String threadSpecName;
    public String guid;

    public ArrayList<EdgeSchema> outgoingEdges;
    public ArrayList<EdgeSchema> incomingEdges;

    public HashMap<String, VariableAssignmentSchema> variables;

    public String externalEventDefName;
    public String externalEventDefGuid;

    public String threadWaitSourceNodeName;
    public String threadWaitSourceNodeGuid;

    public String threadSpawnThreadSpecName;
    public HashMap<String, VariableMutationSchema> variableMutations;

    @JsonBackReference
    public ThreadSpecSchema threadSpec;

    // Everything below doesn't show up in json.
    @JsonIgnore
    public String getK8sName() {
        return LHUtil.toValidK8sName(threadSpec.wfSpec.k8sName + "-" + name);
    }

    @JsonIgnore
    private TaskDefSchema taskDef;

    @JsonIgnore
    private ExternalEventDefSchema externalEventDef;

    @JsonIgnore
    public TaskDefSchema getTaskDef() {
        if (taskDef == null) {
            try {
                taskDef = LHDatabaseClient.lookupTaskDef(taskDefinitionName, config);
            } catch(LHLookupException exn) {
                exn.printStackTrace();
            }
        }
        return taskDef;
    }

    @JsonIgnore
    public ExternalEventDefSchema getExternalEventDef() {
        if (externalEventDef == null) {
            try {
                externalEventDef = LHDatabaseClient.lookupExternalEventDef(
                    externalEventDefGuid, config
                );
            } catch(LHLookupException exn) {
                exn.printStackTrace();
            } catch(NullPointerException exn) {
                exn.printStackTrace();
            }
        }
        return externalEventDef;
    }

    @JsonIgnore
    public String getNamespace() {
        return threadSpec.wfSpec.namespace;
    }

    @JsonIgnore
    public int getReplicas() {
        return config.getDefaultReplicas();
    }

    @JsonIgnore
    public int getPartitions() {
        return config.getDefaultPartitions();
    }

    @JsonIgnore
    public ArrayList<String> getTaskDaemonCommand() {
        return config.getTaskDaemonCommand();
    }

    @JsonIgnore
    private ArrayList<String> getK8sEntrypointCommand() {
        if (nodeType == NodeType.TASK) {
            return getTaskDaemonCommand();
        } else {
            LHUtil.logError("shouldn't be here");
            return null;
        }
    }

    @JsonIgnore
    public Deployment getK8sDeployment() {
        if (nodeType != NodeType.TASK) {
            return null;
            // throw new RuntimeException("Shouldn't be calling getK8sDeployment for non-task nodes");
        }
        Deployment dp = new Deployment();
        dp.metadata = new DeploymentMetadata();
        dp.spec = new DeploymentSpec();
        dp.kind = "Deployment";
        dp.apiVersion = "apps/v1";

        dp.metadata.name = this.getK8sName();
        dp.metadata.labels = new HashMap<String, String>();
        dp.metadata.namespace = threadSpec.wfSpec.namespace;
        dp.metadata.labels.put("app", this.getK8sName());
        dp.metadata.labels.put("littlehorse.io/wfSpecGuid", threadSpec.wfSpec.guid);
        dp.metadata.labels.put("littlehorse.io/nodeGuid", this.guid);
        dp.metadata.labels.put("littlehorse.io/nodeName", this.name);
        dp.metadata.labels.put("littlehorse.io/wfSpecName", threadSpec.wfSpec.name);
        dp.metadata.labels.put("littlehorse.io/active", "true");

        Container container = new Container();
        container.name = this.getK8sName();
        container.image = getTaskDef().dockerImage;
        container.imagePullPolicy = "IfNotPresent";
        container.command = getK8sEntrypointCommand();
        container.env = config.getBaseK8sEnv();
        container.env.add(new EnvEntry(
            Constants.KAFKA_APPLICATION_ID_KEY,
            this.guid
        ));

        container.env.add(new EnvEntry(Constants.WF_SPEC_GUID_KEY, wfSpecGuid));
        container.env.add(new EnvEntry(Constants.NODE_NAME_KEY, name));
        container.env.add(
            new EnvEntry(Constants.THREAD_SPEC_NAME_KEY, threadSpecName));

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.name = this.getK8sName();
        template.metadata.labels = new HashMap<String, String>();
        template.metadata.namespace = threadSpec.wfSpec.namespace;
        template.metadata.labels.put("app", this.getK8sName());
        template.metadata.labels.put("littlehorse.io/wfSpecGuid", threadSpec.wfSpec.guid);
        template.metadata.labels.put("littlehorse.io/nodeGuid", this.guid);
        template.metadata.labels.put("littlehorse.io/nodeName", this.name);
        template.metadata.labels.put("littlehorse.io/wfSpecName", threadSpec.wfSpec.name);
        template.metadata.labels.put("littlehorse.io/active", "true");
        template.metadata.labels.put(
            "littlehorse.io/threadSpecName", this.threadSpecName
        );

        template.spec = new PodSpec();
        template.spec.containers = new ArrayList<Container>();
        template.spec.containers.add(container);

        dp.spec.template = template;
        dp.spec.replicas = this.getReplicas();
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", this.getK8sName());
        dp.spec.selector.matchLabels.put(
            "littlehorse.io/wfSpecGuid", threadSpec.wfSpec.guid
        );
        dp.spec.selector.matchLabels.put("littlehorse.io/nodeGuid", this.guid);
        dp.spec.selector.matchLabels.put("littlehorse.io/nodeName", this.name);
        dp.spec.selector.matchLabels.put(
            "littlehorse.io/threadSpecName", this.threadSpecName
        );
        dp.spec.selector.matchLabels.put(
            "littlehorse.io/wfSpecName", threadSpec.wfSpec.name
        );

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
