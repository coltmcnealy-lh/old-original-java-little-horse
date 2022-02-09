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
import little.horse.common.objects.metadata.ExternalEventDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
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
        ExternalEventDef spec = ctx.bodyAsClass(ExternalEventDef.class);
        spec.setConfig(config);
        spec.validateAndCleanup();
        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.digest = spec.guid;
        response.name = spec.name;
        ctx.json(response);
    }

    public void get(Context ctx) {
        ReadOnlyKeyValueStore<String, ExternalEventDef> nStore = streams.getExternalEventDefNameStore();
        ReadOnlyKeyValueStore<String, ExternalEventDef> gStore = streams.getExternalEventDefGuidStore();
        String id = ctx.pathParam("nameOrGuid");

        ExternalEventDef schemaFromName = nStore.get(id);
        ExternalEventDef schemaFromGuid = gStore.get(id);
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
        WFRun wfRun;
        WFSpec wfSpec;
        ExternalEventDef evd;

        try {
            wfRun = LHDatabaseClient.lookupMeta(wfRunGuid, config);
            wfRun.setConfig(config);
            wfSpec = wfRun.getWFSpec();
            evd = LHDatabaseClient.lookupExternalEventDef(externalEventDefID, config);
        } catch(LHConnectionError exn) {
            ctx.status(404);
            LHAPIError error = new LHAPIError("Orzdash: " + exn.getMessage());
            ctx.json(error);
            return;
        }

        WFRun schema = wfRun;
        ExternalEventDef evdSchema = evd;
        String externalEventGuid = LHUtil.generateGuid();

        ExternalEventPayloadSchema payload = new ExternalEventPayloadSchema();
        payload.externalEventDefGuid = evdSchema.guid;
        payload.externalEventDefName = evdSchema.name;
        payload.externalEventGuid = externalEventGuid;
        payload.content = eventContent;

        WFEventSchema wfEvent = new WFEventSchema();
        wfEvent.wfRunGuid = schema.guid;
        wfEvent.wfSpecDigest = schema.wfSpecDigest;
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
        response.digest = externalEventGuid;
        ctx.json(response);
    }
}
