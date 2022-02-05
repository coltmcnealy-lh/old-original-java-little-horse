package little.horse.common.util;

import java.io.IOException;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHLookupExceptionReason;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.ExternalEventDefSchema;
import little.horse.common.objects.metadata.TaskDefSchema;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.objects.rundata.WFRunSchema;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;


/**
 * This class is a wrapper around the "database" for LittleHorse, which in reality
 * is not a real database: the query layer is the LittleHorse API, and the data is
 * stored in RocksDB by KafkaStreams. To make a read query, you make a request to
 * the LittleHorse API (that is what the public static methods in this class do).
 * 
 * IMPORTANT: 
 */
public class LHDatabaseClient {

    public static WFRunSchema lookupWfRun(String guid, Config config)
    throws LHConnectionError {

        String url = config.getAPIUrlFor(Constants.WF_RUN_API_PATH) + "/" + guid;
        return (WFRunSchema) LookerUpper.lookupSchema(
            url, config, WFRunSchema.class
        );
    }

    public static WFSpecSchema lookupWFSpec(String id, Config config)
    throws LHConnectionError {

        String url = config.getAPIUrlFor(Constants.WF_SPEC_API_PATH) + "/" + id;
        return (WFSpecSchema) LookerUpper.lookupSchema(
            url, config, WFSpecSchema.class
        );
    }

    public static TaskDefSchema lookupTaskDef(
        String id, Config config
    ) throws LHConnectionError {

        String url = config.getAPIUrlFor(Constants.TASK_DEF_API_PATH) + "/" + id;
        return (TaskDefSchema) LookerUpper.lookupSchema(
            url, config, TaskDefSchema.class
        );
    }

    public static ExternalEventDefSchema lookupExternalEventDef(
        String id, Config config) throws LHConnectionError
    {

        String url = config.getAPIUrlFor(
            Constants.EXTERNAL_EVENT_DEF_PATH
        ) + "/" + id;

        return (ExternalEventDefSchema) LookerUpper.lookupSchema(
            url, config, ExternalEventDefSchema.class
        );
    }

    public static ExternalEventDefSchema lookupOrCreateExternalEventDef(
        ExternalEventDefSchema externalEvent,
        String name,
        String guid,
        Config config
    ) throws LHValidationError, LHConnectionError {
        throw new RuntimeException("Implement me!");
    }

    public static TaskDefSchema lookupOrCreateTaskDef(
        TaskDefSchema taskDef,
        String taskDefName,
        String taskDefGuid,
        Config config
    ) throws LHValidationError, LHConnectionError {
        // TODO: Need to do the actual creation.
        if (taskDef != null) {
            taskDef.fillOut(config);
            // TODO: Here we should create it if it don't exist yet.
            if (taskDefName != null && taskDefName == taskDef.name) {
                throw new LHValidationError(
                    "Task Def name doesn't match provided name"
                );
            } else {
                taskDefName = taskDef.name;
            }

            if (taskDefGuid != null && taskDefGuid != taskDef.getDigest()) {
                throw new LHValidationError(
                    "Digest mismatch for provided guid and provided taskdef!"
                );
            } else {
                taskDefGuid = taskDef.getDigest();
            }
        } else {
            String id = taskDefGuid == null ? taskDefName : taskDefGuid;
            if (id == null) {
                throw new LHValidationError(
                    "Node provides neither task def, name, nor guid to look up."
                );
            }

            try {
                taskDef = LHDatabaseClient.lookupTaskDef(
                    id, config
                );
            } catch (LHConnectionError exn) {
                if (exn.getReason() == LHLookupExceptionReason.OBJECT_NOT_FOUND) {
                    throw new LHValidationError(
                        "Failed to find task def from identifier " + id
                    );
                } else {
                    throw exn;
                }
            }
        }
        return taskDef;
    }
}


class LookerUpper {
    public static BaseSchema lookupSchema(
        String url, Config config, Class<? extends BaseSchema> valueType
    ) throws LHConnectionError {

        LHUtil.logBack(
            2, "Making call to URL", url, "to look up", valueType.getName()
        );

        OkHttpClient client = config.getHttpClient();
        Request request = new Request.Builder().url(url).build();
        Response response;
        String responseBody = null;

        try {
            response = client.newCall(request).execute();
            responseBody = response.body().string();
        }
        catch (IOException exn) {
            String err = "Got an error making request to " + url + ": ";
            err += exn.getMessage() + ".\nWas trying to call URL " + url;

            LHUtil.logError(err);
            throw new LHConnectionError(exn, LHLookupExceptionReason.IO_FAILURE, err);
        }

        // Check response code.
        if (response.code() == 404) {
            throw new LHConnectionError(
                null,
                LHLookupExceptionReason.OBJECT_NOT_FOUND,
                "Could not find object at URL " + url
            );
        } else if (response.code() != 200) {
            if (responseBody == null) {
                responseBody = "";
            }
            throw new LHConnectionError(
                null,
                LHLookupExceptionReason.OTHER_ERROR,
                "API Returned an error: " + String.valueOf(response.code()) + " "
                + responseBody
            );
        }

        BaseSchema out = BaseSchema.fromString(
            responseBody, valueType, config, false
        );
        if (out == null) {
            // That means the response we got didn't match the proper schema.
            throw new LHConnectionError(
                null,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an unparseable response: " + responseBody
            );
        }
        return out;
    }
}