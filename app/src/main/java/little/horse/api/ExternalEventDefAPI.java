package little.horse.api;

import org.apache.kafka.clients.producer.ProducerRecord;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIResponsePost;
import little.horse.common.Config;
import little.horse.common.events.ExternalEventPayload;
import little.horse.common.events.WFEvent;
import little.horse.common.events.WFEventType;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.ExternalEventDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;

public class ExternalEventDefAPI {
    private Config config;

    public ExternalEventDefAPI(Config config, APIStreamsContext streams) {
        this.config = config;
    }

    public void post(Context ctx) {
        ExternalEventDef spec = ctx.bodyAsClass(ExternalEventDef.class);
        spec.record();

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.id = spec.getId();
        response.name = spec.name;
        ctx.json(response);
    }

    public void get(Context ctx) {
        throw new RuntimeException("Replace with CoreMetadataAPI");
    }

    // TODO: Need to add a more generic way of splaying out one event to multiple
    // WFRun's.
    public void postEvent(Context ctx) {
        String wfRunId = ctx.pathParam("wfRunId");
        String externalEventDefID = ctx.pathParam("externalEventDefID");
        Object eventContent = ctx.bodyAsClass(Object.class);
        WFRun wfRun;
        WFSpec wfSpec;
        ExternalEventDef evd;

        try {
            wfRun = LHDatabaseClient.lookupWFRun(wfRunId, config);
            wfSpec = wfRun.getWFSpec();
            evd = LHDatabaseClient.lookupMeta(
                externalEventDefID, config, ExternalEventDef.class
            );
        } catch(LHConnectionError exn) {
            ctx.status(404);
            LHAPIError error = new LHAPIError("Orzdash: " + exn.getMessage());
            ctx.json(error);
            return;
        }

        WFRun schema = wfRun;
        ExternalEventDef evdSchema = evd;
        String externalEventId = LHUtil.generateGuid();

        ExternalEventPayload payload = new ExternalEventPayload();
        payload.externalEventDefId = evdSchema.getId();
        payload.externalEventDefName = evdSchema.name;
        payload.content = eventContent;

        WFEvent wfEvent = new WFEvent();
        wfEvent.wfRunId = schema.getId();
        wfEvent.wfSpecDigest = schema.wfSpecDigest;
        wfEvent.wfSpecName = schema.wfSpecName;
        wfEvent.type = WFEventType.EXTERNAL_EVENT;
        wfEvent.timestamp = LHUtil.now();
        wfEvent.content = payload.toString();

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfSpec.kafkaTopic,
            wfEvent.wfRunId,
            wfEvent.toString()
        );
        config.send(record);

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.id = externalEventId;
        ctx.json(response);
    }
}
