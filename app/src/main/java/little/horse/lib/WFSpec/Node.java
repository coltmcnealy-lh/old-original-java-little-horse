package little.horse.lib.WFSpec;

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
import little.horse.lib.TaskDef.TaskDef;

public class Node {
    private NodeSchema schema;
    private Config config;
    private WFSpec wfSpec;
    private TaskDef taskDef;

    public Node(NodeSchema schema, WFSpec wfSpec, Config config) throws LHLookupException {
        this.config = config;
        this.schema = schema;
        this.wfSpec = wfSpec;
        this.taskDef = TaskDef.fromIdentifier(schema.taskDefinitionName, config);
        // TODO: Validate that wfSpec actually has the NodeSchema in it.
    }

    public String getNamespace() {
        return this.wfSpec.getNamespace();
    }

    public String getK8sName() {
        String name = wfSpec.getK8sName() + "-" + schema.name;
        name = LHUtil.toValidK8sName(name);
        return name;
    }

    public int getReplicas() {
        return this.config.getDefaultReplicas();
    }

    public ArrayList<String> getTaskDaemonCommand() {
        return config.getTaskDaemonCommand();
    }

    public Deployment getK8sDeployment() {
        System.out.println("here in getK8sDeployment");
        Deployment dp = new Deployment();
        dp.metadata = new DeploymentMetadata();
        dp.spec = new DeploymentSpec();
        dp.kind = "Deployment";
        dp.apiVersion = "apps/v1";

        dp.metadata.name = this.getK8sName();
        dp.metadata.labels = new HashMap<String, String>();
        dp.metadata.namespace = this.wfSpec.getNamespace();
        dp.metadata.labels.put("app", this.getK8sName());
        dp.metadata.labels.put("little-horse.io/wfSpecGuid", this.wfSpec.getModel().guid);
        dp.metadata.labels.put("little-horse.io/NodeGuid", this.schema.guid);
        dp.metadata.labels.put("little-horse.io/NodeName", this.schema.name);
        dp.metadata.labels.put("little-horse.io/wfSpecName", this.wfSpec.getModel().name);

        Container container = new Container();
        container.name = this.getK8sName();
        container.image = this.taskDef.getModel().dockerImage;
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
        template.metadata.labels.put("little-horse.io/wfSpecGuid", this.wfSpec.getModel().guid);
        template.metadata.labels.put("little-horse.io/NodeGuid", this.schema.guid);
        template.metadata.labels.put("little-horse.io/NodeName", this.schema.name);
        template.metadata.labels.put("little-horse.io/wfSpecName", this.wfSpec.getModel().name);

        template.spec = new PodSpec();
        template.spec.containers = new ArrayList<Container>();
        template.spec.containers.add(container);

        dp.spec.template = template;
        dp.spec.replicas = this.getReplicas();
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", this.getK8sName());
        dp.spec.selector.matchLabels.put("little-horse.io/wfSpecGuid", this.wfSpec.getModel().guid);
        dp.spec.selector.matchLabels.put("little-horse.io/NodeGuid", this.schema.guid);
        dp.spec.selector.matchLabels.put("little-horse.io/NodeName", this.schema.name);
        dp.spec.selector.matchLabels.put("little-horse.io/wfSpecName", this.wfSpec.getModel().name);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            String result = mapper.writeValueAsString(dp);
            System.out.println("The Result:");
            System.out.println(result);
        } catch (JsonProcessingException exn) {
            System.out.println("asdfasdfasdf");
            System.out.println(exn.getMessage());
        }

        return dp;
    }
}
