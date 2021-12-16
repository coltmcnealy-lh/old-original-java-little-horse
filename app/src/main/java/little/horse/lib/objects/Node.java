package little.horse.lib.objects;

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHUtil;
import little.horse.lib.K8sStuff.Container;
import little.horse.lib.K8sStuff.Deployment;
import little.horse.lib.K8sStuff.DeploymentMetadata;
import little.horse.lib.K8sStuff.DeploymentSpec;
import little.horse.lib.K8sStuff.EnvEntry;
import little.horse.lib.K8sStuff.PodSpec;
import little.horse.lib.K8sStuff.Selector;
import little.horse.lib.K8sStuff.Template;
import little.horse.lib.schemas.NodeSchema;

public class Node {
    private NodeSchema schema;
    private Config config;
    private WFSpec wfSpec;
    private TaskDef taskDef;
    private String k8sName;

    public Node(NodeSchema schema, WFSpec wfSpec, Config config) throws LHLookupException {
        this.config = config;
        this.schema = schema;
        this.wfSpec = wfSpec;
        this.taskDef = TaskDef.fromIdentifier(schema.taskDefinitionName, config);

        this.k8sName = wfSpec.getK8sName() + "-" + schema.name;
        k8sName = LHUtil.toValidK8sName(k8sName);

        // TODO: Validate that wfSpec actually has the NodeSchema in it.
    }

    public String getNamespace() {
        return this.wfSpec.getNamespace();
    }

    public String getK8sName() {
        return this.k8sName;
    }

    public int getReplicas() {
        return this.config.getDefaultReplicas();
    }

    public ArrayList<String> getTaskDaemonCommand() {
        return config.getTaskDaemonCommand();
    }

    public Deployment getK8sDeployment() {
        Deployment dp = new Deployment();
        dp.metadata = new DeploymentMetadata();
        dp.spec = new DeploymentSpec();
        dp.kind = "Deployment";
        dp.apiVersion = "apps/v1";

        dp.metadata.name = this.getK8sName();
        dp.metadata.labels = new HashMap<String, String>();
        dp.metadata.namespace = this.wfSpec.getNamespace();
        dp.metadata.labels.put("app", this.getK8sName());
        dp.metadata.labels.put("littlehorse.io/wfSpecGuid", this.wfSpec.getModel().guid);
        dp.metadata.labels.put("littlehorse.io/NodeGuid", this.schema.guid);
        dp.metadata.labels.put("littlehorse.io/NodeName", this.schema.name);
        dp.metadata.labels.put("littlehorse.io/wfSpecName", this.wfSpec.getModel().name);
        dp.metadata.labels.put("littlehorse.io/active", "true");

        Container container = new Container();
        container.name = this.getK8sName();
        container.image = this.taskDef.getModel().dockerImage;
        container.imagePullPolicy = "IfNotPresent";
        container.command = getTaskDaemonCommand();
        container.env = config.getBaseK8sEnv();
        container.env.add(new EnvEntry(
            Constants.KAFKA_APPLICATION_ID_KEY,
            this.schema.guid
        ));
        container.env.add(new EnvEntry(
            Constants.WF_SPEC_GUID_KEY,
            schema.wfSpecGuid
        ));
        container.env.add(new EnvEntry(
            Constants.NODE_NAME_KEY,
            schema.name
        ));

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.name = this.getK8sName();
        template.metadata.labels = new HashMap<String, String>();
        template.metadata.namespace = this.wfSpec.getNamespace();
        template.metadata.labels.put("app", this.getK8sName());
        template.metadata.labels.put("littlehorse.io/wfSpecGuid", this.wfSpec.getModel().guid);
        template.metadata.labels.put("littlehorse.io/NodeGuid", this.schema.guid);
        template.metadata.labels.put("littlehorse.io/NodeName", this.schema.name);
        template.metadata.labels.put("littlehorse.io/wfSpecName", this.wfSpec.getModel().name);
        template.metadata.labels.put("littlehorse.io/active", "true");

        template.spec = new PodSpec();
        template.spec.containers = new ArrayList<Container>();
        template.spec.containers.add(container);

        dp.spec.template = template;
        dp.spec.replicas = this.getReplicas();
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", this.getK8sName());
        dp.spec.selector.matchLabels.put("littlehorse.io/wfSpecGuid", this.wfSpec.getModel().guid);
        dp.spec.selector.matchLabels.put("littlehorse.io/NodeGuid", this.schema.guid);
        dp.spec.selector.matchLabels.put("littlehorse.io/NodeName", this.schema.name);
        dp.spec.selector.matchLabels.put("littlehorse.io/wfSpecName", this.wfSpec.getModel().name);

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
