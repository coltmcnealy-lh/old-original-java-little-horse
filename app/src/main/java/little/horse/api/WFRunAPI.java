package little.horse.api;

import org.apache.kafka.clients.producer.ProducerRecord;

import io.javalin.http.Context;

import little.horse.lib.Config;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.PostWFSpecResponse;
import little.horse.lib.WFEventSchema;
import little.horse.lib.WFRunRequestSchema;
import little.horse.lib.WFSpec;

public class WFRunAPI {
    private Config config;
    private APIStreamsContext wfRunStreams;

    public WFRunAPI(Config config, APIStreamsContext wfRunStreams) {
        this.config = config;
        this.wfRunStreams = wfRunStreams;
    }

    public void get(Context ctx) {
        // TODO: Need to add a KafkaStreams topology which watches for this and collects it.
    }

    public void post(Context ctx) {
        WFRunRequestSchema request = ctx.bodyAsClass(WFRunRequestSchema.class);
        String wfSpecId = ctx.pathParam("wfSpec");
        WFSpec wfSpec = null;

        WFEventSchema event = new WFEventSchema();

        try {
            wfSpec = WFSpec.fromIdentifier(wfSpecId, config);
        } catch (LHLookupException exn) {
            ctx.status(404);
            LHAPIError err = new LHAPIError("Unable to find desired wfSpec: " + exn.getMessage());
            ctx.json(err);
            return;
        } catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError err = new LHAPIError(
                "Failed looking up provided wfSpec: " + exn.getMessage()
            );
            ctx.json(err);
            return;
        }
        String guid = LHUtil.generateGuid();
        event.wfRunGuid = guid;
        event.wfSpecGuid = wfSpec.getModel().guid;
        event.wfSpecName = wfSpec.getModel().name;
        event.content = request.toString();

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfSpec.getModel().kafkaTopic,
            event.wfRunGuid,
            event.toString()
        );

        config.send(record); // TODO: Some checking that it went through.

        PostWFSpecResponse response = new PostWFSpecResponse();
        response.guid = guid;
        response.status = LHStatus.PENDING;

        ctx.status(201);
        ctx.json(response);
    }
}
