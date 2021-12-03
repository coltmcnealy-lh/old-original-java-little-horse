package little.horse.lib.WFSpec;

import little.horse.lib.Config;
import little.horse.lib.LHUtil;

import java.util.ArrayList;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

public class WFSpec {
    private Config config;
    private WFSpecSchema schema;

    public WFSpec(WFSpecSchema schema, Config config) {
        // TODO (hard): do some validation that we don't have a 409.
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        for (Map.Entry<String, NodeSchema> pair: schema.nodes.entrySet()) {
            pair.getValue().wfSpecGuid = schema.guid;
            if (pair.getValue().guid == null) {
                pair.getValue().guid = LHUtil.generateGuid();
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
        }

        this.schema = schema;
        this.config = config;
    }

    public WFSpecSchema getModel() {
        return this.schema;
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

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getWFSpecTopic(),
            schema.guid,
            this.toString()
        );
        this.config.send(record);
    }
}
