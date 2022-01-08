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
import little.horse.lib.WFEventType;
import little.horse.lib.K8sStuff.Deployment;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.SignalHandlerSpecSchema;
import little.horse.lib.schemas.VariableAssignmentSchema;
import little.horse.lib.schemas.WFSpecSchema;
import little.horse.lib.schemas.WFTriggerSchema;
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
            schema.kafkaTopic = config.getWFRunTopicPrefix() + schema.name + "_" + schema.guid;
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

            if (node.outgoingEdges == null) {
                node.outgoingEdges = new ArrayList<EdgeSchema>();
            }

            // Now validate that the TaskDef's actually exist
            if (node.nodeType == NodeType.TASK) {
                if (node.taskDefinitionName == null) {
                    throw new LHValidationError("Invalid Node " + node.name + ": No taskDefinition supplied");
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

                if (node.variables != null) {
                    for (String varName : node.variables.keySet()) {
                        VariableAssignmentSchema var = node.variables.get(varName);

                        if (var.wfRunVariableName != null) {
                            if (schema.variableDefs == null) {
                                throw new LHValidationError(
                                    "Need to provide VariableDef since it's used on node " + node.name + " for variable " + varName
                                );
                            }
                            if (schema.variableDefs.get(var.wfRunVariableName) == null) {
                                throw new LHValidationError(
                                    "Referenced nonexistent wfrunvariable " + var.wfRunVariableName
                                );
                            }
                        }
                    }
                }

            } else {
                if (node.nodeType != NodeType.EXTERNAL_EVENT) {
                    throw new RuntimeException("oops");
                }

                ExternalEventDef eed = null;
                if (node.externalEventDefGuid != null) {
                    try {
                        eed = ExternalEventDef.fromIdentifier(node.externalEventDefGuid, config);
                    } catch (LHLookupException exn) {
                        throw new LHValidationError("Could not find externaleventdef " + node.externalEventDefGuid);
                    }
                } else if (node.externalEventDefName != null) {
                    try {
                        eed = ExternalEventDef.fromIdentifier(node.externalEventDefName, config);
                    } catch (LHLookupException exn) {
                        exn.printStackTrace();
                        throw new LHValidationError("Could not find externaleventdef " + node.externalEventDefName);
                    }
                }

                node.externalEventDefGuid = eed.getModel().guid;
                node.externalEventDefName = eed.getModel().name;
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

            boolean alreadyHasEdge = false;
            for (EdgeSchema candidate : source.outgoingEdges) {
                if (candidate.sinkNodeName.equals(sink.name)) {
                    alreadyHasEdge = true;
                    break;
                }
            }
            if (!alreadyHasEdge) {
                source.outgoingEdges.add(edge);
            }

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
                trigger.triggerEventType = WFEventType.NODE_COMPLETED;
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

        if (schema.signalHandlers == null) {
            schema.signalHandlers = new ArrayList<SignalHandlerSpecSchema>();
        }
        for (SignalHandlerSpecSchema handler : schema.signalHandlers) {
            ExternalEventDef eed = null;
            if (handler.externalEventDefGuid != null) {
                try {
                    eed = ExternalEventDef.fromIdentifier(handler.externalEventDefGuid, config);
                } catch (LHLookupException exn) {
                    throw new LHValidationError("Could not find externaleventdef " + handler.externalEventDefGuid);
                }
            } else if (handler.externalEventDefName != null) {
                try {
                    eed = ExternalEventDef.fromIdentifier(handler.externalEventDefName, config);
                } catch (LHLookupException exn) {
                    exn.printStackTrace();
                    throw new LHValidationError("Could not find externaleventdef " + handler.externalEventDefName);
                }
            }
            handler.externalEventDefGuid = eed.getModel().guid;
            handler.externalEventDefName = eed.getModel().name;

            WFSpec handlerSpec = null;
            try {
                if (handler.wfSpecGuid != null) {
                    handlerSpec = WFSpec.fromIdentifier(handler.wfSpecGuid, config);
                } else {
                    handlerSpec = WFSpec.fromIdentifier(handler.wfSpecName, config);
                }
            } catch (LHLookupException exn){
                exn.printStackTrace();
                throw new LHValidationError(
                    "couldn't find designated wfSpec on signalHandler for event " + eed.getModel().name +
                    " and the wfSpec name is " + handler.wfSpecName + " " + handler.wfSpecGuid
                );
            }
            handler.wfSpecGuid = handlerSpec.getModel().guid;
            handler.wfSpecName = handlerSpec.getModel().name;
        }

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
        for (Map.Entry<String, NodeSchema> entry : schema.nodes.entrySet()) {
            try {
                list.add(new Node(entry.getValue(), this, config));
            } catch (LHLookupException exn) {
                LHUtil.logError("Shouldn't be possible to have this but we do.", exn.getMessage());
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
