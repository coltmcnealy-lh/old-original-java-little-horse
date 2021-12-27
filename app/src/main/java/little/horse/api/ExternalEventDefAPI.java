package little.horse.api;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.lib.Config;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.WFEventType;
import little.horse.lib.objects.ExternalEventDef;
import little.horse.lib.objects.WFRun;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.LHAPIResponsePost;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.ExternalEventDefSchema;
import little.horse.lib.schemas.ExternalEventPayloadSchema;

public class ExternalEventDefAPI {
    private Config config;
    private APIStreamsContext streams;

    public ExternalEventDefAPI(Config config, APIStreamsContext streams) {
        this.config = config;
        this.streams = streams;
    }

    public void post(Context ctx) {
        ExternalEventDefSchema rawSpec = ctx.bodyAsClass(ExternalEventDefSchema.class);
        ExternalEventDef spec;

        try {
            spec = new ExternalEventDef(rawSpec, this.config);
        }
        catch (LHValidationError exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError(exn.getMessage());
            ctx.json(error);
            return;
        }

        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = spec.getModel().guid;
        response.name = spec.getModel().name;
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
        WFRun wfRun;
        WFSpec wfSpec;
        ExternalEventDef evd;

        try {
            wfRun = WFRun.fromGuid(wfRunGuid, config);
            wfSpec = wfRun.getWFSpec();
            evd = ExternalEventDef.fromIdentifier(externalEventDefID, config);
        } catch(LHLookupException exn) {
            ctx.status(404);
            LHAPIError error = new LHAPIError("Orzdash: " + exn.getMessage());
            ctx.json(error);
            return;
        } catch(LHValidationError exn) {
            ctx.status(400);
            LHAPIError error = new LHAPIError("Orzdash: " + exn.getMessage());
            ctx.json(error);
            return;
        }

        WFRunSchema schema = wfRun.getModel();
        ExternalEventDefSchema evdSchema = evd.getModel();
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
            wfSpec.getModel().kafkaTopic,
            wfEvent.wfRunGuid,
            wfEvent.toString()
        );
        config.send(record);

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = externalEventGuid;
        ctx.json(response);
    }
}
