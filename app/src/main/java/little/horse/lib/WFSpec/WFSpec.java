package little.horse.lib.WFSpec;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHDeployError;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHLookupExceptionReason;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.K8sStuff.Deployment;
import little.horse.lib.TaskDef.TaskDef;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.clients.producer.ProducerRecord;


public class WFSpec {
    private Config config;
    private WFSpecSchema schema;

    public WFSpec(WFSpecSchema schema, Config config) throws LHValidationError {
        // TODO (hard): do some validation that we don't have a 409.
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.inputKafkaTopic == null) {
            schema.inputKafkaTopic = config.getKafkaTopicPrefix() + schema.name + "_" + schema.guid;
        }

        if (schema.status == null) {
            schema.status = LHStatus.STOPPED;
        }

        if (schema.desiredStatus == null) {
            schema.desiredStatus = LHStatus.RUNNING;
        }

        for (Map.Entry<String, NodeSchema> pair: schema.nodes.entrySet()) {
            NodeSchema node = pair.getValue();
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

            if (node.outputKafkaTopic == null) {
                node.outputKafkaTopic = config.getKafkaTopicPrefix() + node.guid + "_output";
            }
            if (node.interruptKafkaTopic == null) {
                node.interruptKafkaTopic = config.getKafkaTopicPrefix() + node.guid + "_interrupt";
            }
            if (node.inputKafkaTopics == null) {
                node.inputKafkaTopics = new ArrayList<String>();
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

            if (sink.inputKafkaTopics.indexOf(source.outputKafkaTopic) == -1) {
                sink.inputKafkaTopics.add(source.outputKafkaTopic);
            }
        }

        // Lastly, find the (supposedly exactly one) node which has zero input topics, and
        // deem that node the entrypoint.
        NodeSchema entrypoint = null;
        for (Map.Entry<String, NodeSchema> pair: schema.nodes.entrySet()) {
            NodeSchema node = pair.getValue();
            if (node.inputKafkaTopics.size() == 0) {
                if (entrypoint != null) {
                    throw new LHValidationError(
                        "Invalid WFSpec: More than one node without incoming edges."
                    );
                }
                entrypoint = node;
            }
            if (entrypoint == null) {
                throw new LHValidationError("No entrypoint node provided!");
            }
            schema.entrypointNodeName = node.name;
            node.inputKafkaTopics.add(schema.inputKafkaTopic);
        }

        this.schema = schema;
        this.config = config;
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
        return LHUtil.toValidK8sName(this.schema.name);
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

    public void deploy() throws LHDeployError {
        // First, create the kafka topics

        // Next, deploy the kafkaStreams collector

        // Finally, 

        for (Node node : this.getNodes()) {
            Deployment deployment = node.getK8sDeployment();
            String yml;
            try {
                yml = new ObjectMapper(new YAMLFactory()).writeValueAsString(deployment);

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
