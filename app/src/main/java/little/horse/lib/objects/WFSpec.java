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

        if (node.outgoingEdges == null) {
            node.outgoingEdges = new ArrayList<EdgeSchema>();
        }
        if (node.incomingEdges == null) {
            node.incomingEdges = new ArrayList<EdgeSchema>();
        }

        if (node.nodeType == NodeType.TASK) {
            cleanupTaskNode(node);
        } else if (node.nodeType == NodeType.EXTERNAL_EVENT) {
            cleanupExternalEventNode(node);
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

        for (Map.Entry<String, NodeSchema> pair: spec.nodes.entrySet()) {
            // clean up the node (i.e. give it a guid if it doesn't have one, etc), validate that
            // its taskdef exist
            cleanupNode(pair.getKey(), pair.getValue(), spec);
        }

        if (spec.edges == null) {
            spec.edges = new ArrayList<EdgeSchema>();
        }
        for (EdgeSchema edge : spec.edges) {
            cleanupEdge(edge, spec);
        }

        // Lastly, assign the entrypoint node
        if (spec.entrypointNodeName == null) {
            spec.entrypointNodeName = calculateEntrypointNode(spec);
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

        for (Map.Entry<String, ThreadSpecSchema> pair: schema.threadSpecs.entrySet()) {
            cleanupThreadSpec(pair.getValue(), pair.getKey());
        }

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

        WFSpecSchema schema = BaseSchema.fromString(responseBody, WFSpecSchema.class);
        if (schema == null) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an invalid response: " + responseBody
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
