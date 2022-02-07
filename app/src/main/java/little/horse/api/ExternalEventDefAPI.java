package little.horse.api;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIResponsePost;
import little.horse.common.Config;
import little.horse.common.events.ExternalEventPayloadSchema;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.ExternalEventDefSchema;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.objects.rundata.WFRunSchema;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;

public class ExternalEventDefAPI {
    private Config config;
    private APIStreamsContext streams;

    public ExternalEventDefAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        ExternalEventDefSchema spec = ctx.bodyAsClass(ExternalEventDefSchema.class);
        spec.setConfig(config);
        spec.validateAndCleanup();
        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = spec.guid;
        response.name = spec.name;
        ctx.json(response);
    }

    public void get(Context ctx) {
        ReadOnlyKeyValueStore<String, ExternalEventDefSchema> nStore = streams.getExternalEventDefNameStore();
        ReadOnlyKeyValueStore<String, ExternalEventDefSchema> gStore = streams.getExternalEventDefGuidStore();
        String id = ctx.pathParam("nameOrGuid");

        ExternalEventDefSchema schemaFromName = nStore.get(id);
        ExternalEventDefSchema schemaFromGuid = gStore.get(id);
        if (schemaFromName != null) {
            ctx.json(schemaFromName);
            return;
        }
        if (schemaFromGuid != null) {
            ctx.json(schemaFromGuid);
            return;
        }

        LHAPIError error = new LHAPIError(
            "Could not find ExternalEventDef with identifier " + id
        );
        ctx.status(404);
        ctx.json(error);
    }

    // TODO: Need to add a more generic way of splaying out one event to multiple
    // WFRun's.
    public void postEvent(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        String externalEventDefID = ctx.pathParam("externalEventDefID");
        Object eventContent = ctx.bodyAsClass(Object.class);
        WFRunSchema wfRun;
        WFSpecSchema wfSpec;
        ExternalEventDefSchema evd;

        try {
            wfRun = LHDatabaseClient.lookupWfRun(wfRunGuid, config);
            wfRun.setConfig(config);
            wfSpec = wfRun.getWFSpec();
            evd = LHDatabaseClient.lookupExternalEventDef(externalEventDefID, config);
        } catch(LHConnectionError exn) {
            ctx.status(404);
            LHAPIError error = new LHAPIError("Orzdash: " + exn.getMessage());
            ctx.json(error);
            return;
        }

        WFRunSchema schema = wfRun;
        ExternalEventDefSchema evdSchema = evd;
        String externalEventGuid = LHUtil.generateGuid();

        ExternalEventPayloadSchema payload = new ExternalEventPayloadSchema();
        payload.externalEventDefGuid = evdSchema.guid;
        payload.externalEventDefName = evdSchema.name;
        payload.externalEventGuid = externalEventGuid;
        payload.content = eventContent;

        WFEventSchema wfEvent = new WFEventSchema();
        wfEvent.wfRunGuid = schema.guid;
        wfEvent.wfSpecGuid = schema.wfSpecGuid;
        wfEvent.wfSpecName = schema.wfSpecName;
        wfEvent.type = WFEventType.EXTERNAL_EVENT;
        wfEvent.timestamp = LHUtil.now();
        wfEvent.content = payload.toString();

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfSpec.kafkaTopic,
            wfEvent.wfRunGuid,
            wfEvent.toString()
        );
        config.send(record);

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = externalEventGuid;
        ctx.json(response);
    }
}
