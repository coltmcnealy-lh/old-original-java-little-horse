package little.horse.api;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;

import little.horse.lib.Config;
import little.horse.lib.LHDatabaseClient;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.WFEventType;
import little.horse.lib.schemas.LHAPIResponsePost;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunRequestSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFSpecSchema;

public class WFRunAPI {
    private Config config;
    private APIStreamsContext wfRunStreams;

    public WFRunAPI(Config config, APIStreamsContext wfRunStreams) {
        this.config = config;
        this.wfRunStreams = wfRunStreams;
    }

    public void get(Context ctx) {
        ReadOnlyKeyValueStore<String, WFRunSchema> store = wfRunStreams.getWFRunStore();
        String wfRunGuid = ctx.pathParam("wfRunGuid");

        WFRunSchema wfRun = store.get(wfRunGuid);
        if (wfRun == null) {
            ctx.status(404);
            return;
        }

        ctx.json(wfRun);
    }

    public void post(Context ctx) {
        WFRunRequestSchema request = ctx.bodyAsClass(WFRunRequestSchema.class);
        String wfSpecId = ctx.pathParam("wfSpec");
        WFSpecSchema wfSpec = null;

        WFEventSchema event = new WFEventSchema();

        try {
            wfSpec = LHDatabaseClient.lookupWFSpec(wfSpecId, config);
        } catch (LHLookupException exn) {
            ctx.status(404);
            LHAPIError err = new LHAPIError(
                "Unable to find desired wfSpec: " + exn.getMessage());
            ctx.json(err);
            return;
        }
        String guid = LHUtil.generateGuid();
        event.wfRunGuid = guid;
        event.wfSpecGuid = wfSpec.guid;
        event.wfSpecName = wfSpec.name;
        event.content = request.toString();
        event.type = WFEventType.WF_RUN_STARTED;

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfSpec.kafkaTopic,
            event.wfRunGuid,
            event.toString()
        );

        config.send(record); // TODO: Some checking that it went through.

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = guid;
        response.status = LHStatus.PENDING;

        ctx.status(201);
        ctx.json(response);
    }
}
