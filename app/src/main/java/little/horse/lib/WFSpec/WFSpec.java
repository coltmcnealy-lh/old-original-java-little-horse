package little.horse.lib.WFSpec;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHDeployError;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHLookupExceptionReason;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.K8sStuff.Container;
import little.horse.lib.K8sStuff.Deployment;
import little.horse.lib.K8sStuff.DeploymentMetadata;
import little.horse.lib.K8sStuff.DeploymentSpec;
import little.horse.lib.K8sStuff.EnvEntry;
import little.horse.lib.K8sStuff.PodSpec;
import little.horse.lib.K8sStuff.Selector;
import little.horse.lib.K8sStuff.Service;
import little.horse.lib.K8sStuff.ServicePort;
import little.horse.lib.K8sStuff.ServiceSpec;
import little.horse.lib.K8sStuff.Template;
import little.horse.lib.TaskDef.TaskDef;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
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

    public WFSpec(WFSpecSchema schema, Config config) throws LHValidationError {
        // TODO (hard): do some validation that we don't have a 409.
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.kafkaTopic == null) {
            schema.kafkaTopic = config.getKafkaTopicPrefix() + schema.name + "_" + schema.guid;
        }

        if (schema.status == null) {
            schema.status = LHStatus.STOPPED;
        }

        if (schema.desiredStatus == null) {
            schema.desiredStatus = LHStatus.RUNNING;
        }

        for (Map.Entry<String, NodeSchema> pair: schema.nodes.entrySet()) {
            NodeSchema node = pair.getValue();
            if (node.triggers == null) {
                node.triggers = new ArrayList<WFTriggerSchema>();
            }
            node.wfSpecGuid = schema.guid;
            if (node.guid == null) {
                node.guid = LHUtil.generateGuid();
            }
            
            if (node.name == null) {
                node.name = pair.getKey();
            } else if (!node.name.equals(pair.getKey())) {
                throw new LHValidationError("Node name didn't match for node " + pair.getKey());
            }

            // Now validate that the TaskDef's actually exist
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
                        "No task definition named " + node.taskDefinitionName + " found."
                    );
                } else {
                    throw new LHValidationError(
                        "Failed looking up TaskDef " + node.taskDefinitionName
                    );
                }
            }

        }

        if (schema.edges == null) {
            schema.edges = new ArrayList<EdgeSchema>();
        }
        // TODO: Some error handling here if bad spec provided.
        for (EdgeSchema edge : schema.edges) {
            edge.wfSpecGuid = schema.guid;
            if (edge.guid == null) {
                edge.guid = LHUtil.generateGuid();
            }
            NodeSchema source = schema.nodes.get(edge.sourceNodeName);
            NodeSchema sink = schema.nodes.get(edge.sinkNodeName);
            edge.sourceNodeGuid = source.guid;
            edge.sinkNodeGuid = sink.guid;

            // Add a WFTrigger to the triggers list.
            boolean found = false;
            for (WFTriggerSchema trigger : sink.triggers) {
                if (trigger.triggerNodeGuid.equals(source.guid) || 
                    trigger.triggerNodeName.equals(source.name)
                ) {
                    // TODO: if found is already true, raise a big stink.
                    found = true;
                }
            }
            if (!found) {
                WFTriggerSchema trigger = new WFTriggerSchema();
                trigger.triggerEventType = WFEventType.TASK_COMPLETED;
                trigger.triggerNodeName = source.name;
                trigger.triggerNodeGuid = source.guid;
                sink.triggers.add(trigger);
            }
        }

        // Lastly, find the (supposedly exactly one) node which has zero input topics, and
        // deem that node the entrypoint.
        NodeSchema entrypoint = null;
        for (Map.Entry<String, NodeSchema> pair: schema.nodes.entrySet()) {
            NodeSchema node = pair.getValue();
            if (node.triggers.size() == 0) {
                entrypoint = node;

                WFTriggerSchema trigger = new WFTriggerSchema();
                trigger.triggerEventType = WFEventType.WF_RUN_STARTED;
                node.triggers.add(trigger);

            } else if (node.triggers.size() == 1 &&
                node.triggers.get(0).triggerEventType == WFEventType.WF_RUN_STARTED
            ) {
                if (entrypoint != null) {
                    throw new LHValidationError(
                        "Invalid WFSpec: More than one node without incoming edges."
                    );
                }
                entrypoint = node;
            }
        } // for node in schema.nodes

        if (entrypoint == null) {
            throw new LHValidationError("No entrypoint node provided!");
        }
        schema.entrypointNodeName = entrypoint.name;

        this.schema = schema;
        this.config = config;
        this.k8sName = LHUtil.toValidK8sName(
            this.schema.name + "-" + LHUtil.digestify(schema.guid)
        );
    }

    /**
     * Loads a WFSpec object by querying the backend data store (as of this commit, that
     * means making a REST call to the main API running KafkaStreams).
     * @param guid the guid to load.
     * @param config a valid little.horse.lib.Config object representing this environment.
     * @return a WFSpec object.
     */
    public static WFSpec fromIdentifier(String guid, Config config) 
    throws LHLookupException, LHValidationError {

        OkHttpClient client = config.getHttpClient();
        String url = config.getAPIUrlFor(Constants.WF_SPEC_API_PATH) + "/" + guid;
        Request request = new Request.Builder().url(url).build();
        Response response;
        String responseBody = null;
        System.out.println("Calling url: " + url);

        try {
            response = client.newCall(request).execute();
            responseBody = response.body().string();
        }
        catch (IOException exn) {
            String err = "Got an error making request to " + url + ": " + exn.getMessage() + ".\n";
            err += "Was trying to call URL " + url;

            System.err.println(err);
            throw new LHLookupException(exn, LHLookupExceptionReason.IO_FAILURE, err);
        }

        // Check response code.
        if (response.code() == 404) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OBJECT_NOT_FOUND,
                "Could not find WFSpec with guid " + guid + "."
            );
        } else if (response.code() != 200) {
            if (responseBody == null) {
                responseBody = "";
            }
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OTHER_ERROR,
                "API Returned an error: " + String.valueOf(response.code()) + " " + responseBody
            );
        }

        WFSpecSchema schema = null;
        try {
            schema = new ObjectMapper().readValue(responseBody, WFSpecSchema.class);
        } catch (JsonProcessingException exn) {
            System.err.println(
                "Got an invalid response: " + exn.getMessage() + " " + responseBody
            );
            throw new LHLookupException(
                exn,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an invalid response: " + responseBody + " " + exn.getMessage()
            );
        }

        return new WFSpec(schema, config);
    }

    public WFSpecSchema getModel() {
        return this.schema;
    }

    public String getNamespace() {
        return "default";
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String result;
        try {
            result = mapper.writeValueAsString(this.getModel());
        } catch(JsonProcessingException exn) {
            System.out.println(exn.toString());
            result = "Could not serialize.";
        }
        return result;
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
        for (Map.Entry<String, NodeSchema> entry : schema.nodes.entrySet()) {
            try {
                list.add(new Node(entry.getValue(), this, config));
            } catch (LHLookupException exn) {
                System.out.println("oops");
                System.out.println(exn.getMessage());
                // Nothing to do, because shouldn't be able to get here.
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

    public Deployment getCollectorDeployment() {
        Deployment dp = new Deployment();
        dp.metadata = new DeploymentMetadata();
        dp.spec = new DeploymentSpec();
        dp.kind = "Deployment";
        dp.apiVersion = "apps/v1";

        dp.metadata.name = this.getK8sName();
        dp.metadata.labels = new HashMap<String, String>();
        dp.metadata.namespace = this.getNamespace();
        dp.metadata.labels.put("app", this.getK8sName());
        dp.metadata.labels.put("littlehorse.io/wfSpecGuid", this.getModel().guid);
        dp.metadata.labels.put("littlehorse.io/wfSpecName", this.getModel().name);
        dp.metadata.labels.put("littlehorse.io/active", "true");

        Container container = new Container();
        container.name = this.getK8sName();
        container.image = config.getCollectorImage();
        container.imagePullPolicy = "IfNotPresent";
        container.command = config.getCollectorCommand();
        container.env = config.getBaseK8sEnv();
        container.env.add(new EnvEntry(
            Constants.KAFKA_APPLICATION_ID_KEY,
            this.schema.guid
        ));
        container.env.add(new EnvEntry(
            Constants.WF_SPEC_GUID_KEY,
            this.schema.guid
        ));

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.name = this.getK8sName();
        template.metadata.labels = new HashMap<String, String>();
        template.metadata.namespace = this.getNamespace();
        template.metadata.labels.put("app", this.getK8sName());
        template.metadata.labels.put("littlehorse.io/wfSpecGuid", this.getModel().guid);
        template.metadata.labels.put("littlehorse.io/wfSpecName", this.getModel().name);
        template.metadata.labels.put("littlehorse.io/active", "true");

        template.spec = new PodSpec();
        template.spec.containers = new ArrayList<Container>();
        template.spec.containers.add(container);

        dp.spec.template = template;
        dp.spec.replicas = this.getReplicationFactor();
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", this.getK8sName());
        dp.spec.selector.matchLabels.put("littlehorse.io/wfSpecGuid", this.getModel().guid);
        dp.spec.selector.matchLabels.put("littlehorse.io/wfSpecName", this.getModel().name);
        
        return dp;
    }

    public Service getCollectorService() {
        Service svc = new Service();
        svc.spec = new ServiceSpec();
        svc.kind = "Service";
        svc.apiVersion = "v1";
        svc.metadata = new DeploymentMetadata();

        svc.spec.ports = new ArrayList<ServicePort>();
        ServicePort p = new ServicePort();
        p.port = Constants.EXPOSED_PORT;
        p.targetPort = Constants.EXPOSED_PORT;
        p.protocol = "TCP";
        p.name = "http";
        svc.spec.ports.add(p);

        svc.metadata.namespace = this.getNamespace();
        svc.metadata.name = this.getK8sName();
        svc.metadata.labels = new HashMap<String, String>();
        svc.metadata.labels.put("app", this.getK8sName());
        svc.metadata.labels.put("littlehorse.io/wfSpecGuid", this.getModel().guid);
        svc.metadata.labels.put("littlehorse.io/wfSpecName", this.getModel().name);
        svc.metadata.labels.put("littlehorse.io/active", "true");

        HashMap<String, String> selector = new HashMap<String, String>();
        selector.put("app", this.getK8sName());
        selector.put("littlehorse.io/wfSpecGuid", this.getModel().guid);
        selector.put("littlehorse.io/wfSpecName", this.getModel().name);
        svc.spec.selector = selector;

        return svc;
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
                ymlStrings.add(new ObjectMapper(new YAMLFactory()).writeValueAsString(
                    node.getK8sDeployment()
                ));
            } catch (JsonProcessingException exn) {
                exn.printStackTrace();
                throw new LHDeployError("Had an orzdash");
            }
        }

        for (String yml : ymlStrings) {
            try {
                System.out.println(yml + "\n\n\n");
                Process process = Runtime.getRuntime().exec("kubectl apply -f -");

                process.getOutputStream().write(yml.getBytes());
                process.getOutputStream().close();
                process.waitFor();

                BufferedReader input = new BufferedReader(
                    new InputStreamReader(process.getInputStream())
                );
                String line = null;
                while ((line = input.readLine()) != null) {
                    System.out.println(line);
                }

                BufferedReader error = new BufferedReader(
                    new InputStreamReader(process.getErrorStream())
                );
                line = null;
                while ((line = error.readLine()) != null) {
                    System.out.println(line);
                }

            } catch (Exception exn) {
                exn.printStackTrace();
                throw new LHDeployError("had an orzdash");
            }
        }
        this.schema.status = LHStatus.RUNNING;
        this.record();
    }

}
