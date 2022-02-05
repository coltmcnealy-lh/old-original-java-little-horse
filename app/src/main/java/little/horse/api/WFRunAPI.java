package little.horse.api;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.javalin.http.Context;
import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIError;
import little.horse.api.util.LHAPIResponsePost;
import little.horse.common.Config;
import little.horse.common.events.WFEventSchema;
import little.horse.common.events.WFEventType;
import little.horse.common.events.WFRunRequestSchema;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.rundata.LHStatus;
import little.horse.common.objects.rundata.WFRunSchema;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;

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
        // String wfSpecId = ctx.pathParam("wfSpec");
        if (request.wfSpec != null) {
            try {
                request.wfSpec.cleanupAndValidate(config);
            } catch (LHValidationError exn) {
                ctx.status(400);
                LHAPIError err = new LHAPIError(exn.getMessage());
                ctx.json(err);
                return;
            }
        }

        WFEventSchema event = new WFEventSchema();

        // try {
        //     wfSpec = LHDatabaseClient.lookupWFSpec(wfSpecId, config);
        // } catch (LHLookupException exn) {
        //     ctx.status(404);
        //     LHAPIError err = new LHAPIError(
        //         "Unable to find desired wfSpec: " + exn.getMessage());
        //     ctx.json(err);
        //     return;
        // }

        String guid = LHUtil.generateGuid();
        event.wfRunGuid = guid;
        // event.wfSpecGuid = wfSpec.guid;
        // event.wfSpecName = wfSpec.name;
        event.content = request.toString();
        event.type = WFEventType.WF_RUN_STARTED;

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            config.getWFRunTopic(), // wfSpec.kafkaTopic,
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

    public void stopWFRun(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        WFEventSchema event = new WFEventSchema();
        event.wfRunGuid = wfRunGuid;
        event.threadID = 0;
        event.type = WFEventType.WF_RUN_STOP_REQUEST;

        try {
            event.wfRun = LHDatabaseClient.lookupWfRun(wfRunGuid, config);
            event.wfSpecGuid = event.wfRun.wfSpecGuid;
            event.wfSpecName = event.wfRun.wfSpecName;
            event.record();
        } catch(Exception exn) {
            exn.printStackTrace();
            ctx.status(500);
            LHAPIError error = new LHAPIError(
                "Failed recording stop request: " + exn.getMessage()
            );
            ctx.json(error);
            return;
        }

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = wfRunGuid;
        response.status = null;

        ctx.json(response);
    }

    public void resumeWFRun(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        WFEventSchema event = new WFEventSchema();
        event.setConfig(config);
        event.wfRunGuid = wfRunGuid;
        event.type = WFEventType.WF_RUN_RESUME_REQUEST;

        try {
            event.wfRun = LHDatabaseClient.lookupWfRun(wfRunGuid, config);
            event.threadID = 0;
            event.wfSpecGuid = event.wfRun.wfSpecGuid;
            event.wfSpecName = event.wfRun.wfSpecName;
            event.record();
        } catch(Exception exn) {
            exn.printStackTrace();
            ctx.status(500);
            LHAPIError error = new LHAPIError(
                "Failed recording stop request: " + exn.getMessage()
            );
            ctx.json(error);
            return;
        }

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = wfRunGuid;
        response.status = null;

        ctx.json(response);
    }

    public void stopThread(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        int tid = Integer.valueOf(ctx.pathParam("tid"));
        WFEventSchema event = new WFEventSchema();
        event.setConfig(config);
        event.wfRunGuid = wfRunGuid;
        event.type = WFEventType.WF_RUN_STOP_REQUEST;

        try {
            event.wfRun = LHDatabaseClient.lookupWfRun(wfRunGuid, config);
            event.threadID = tid;
            event.wfSpecGuid = event.wfRun.wfSpecGuid;
            event.wfSpecName = event.wfRun.wfSpecName;
            event.record();
        } catch(Exception exn) {
            exn.printStackTrace();
            ctx.status(500);
            LHAPIError error = new LHAPIError(
                "Failed recording stop request: " + exn.getMessage()
            );
            ctx.json(error);
            return;
        }

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = wfRunGuid;
        response.status = null;

        ctx.json(response);
    }

    public void resumeThread(Context ctx) {
        String wfRunGuid = ctx.pathParam("wfRunGuid");
        int tid = Integer.valueOf(ctx.pathParam("tid"));
        WFEventSchema event = new WFEventSchema();
        event.setConfig(config);
        event.wfRunGuid = wfRunGuid;
        event.type = WFEventType.WF_RUN_RESUME_REQUEST;

        try {
            event.wfRun = LHDatabaseClient.lookupWfRun(wfRunGuid, config);
            event.wfSpecGuid = event.wfRun.wfSpecGuid;
            event.threadID = tid;
            event.wfSpecName = event.wfRun.wfSpecName;
            event.record();
        } catch(Exception exn) {
            exn.printStackTrace();
            ctx.status(500);
            LHAPIError error = new LHAPIError(
                "Failed recording stop request: " + exn.getMessage()
            );
            ctx.json(error);
            return;
        }

        LHAPIResponsePost response = new LHAPIResponsePost();
        response.guid = wfRunGuid;
        response.status = null;

        ctx.json(response);
    }

}
