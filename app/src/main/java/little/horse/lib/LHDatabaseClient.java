package little.horse.lib;

import java.io.IOException;

import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.ExternalEventDefSchema;
import little.horse.lib.schemas.TaskDefSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFSpecSchema;
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
    throws LHLookupException {

        String url = config.getAPIUrlFor(Constants.WF_RUN_API_PATH) + "/" + guid;
        return (WFRunSchema) LookerUpper.lookupSchema(
            url, config, WFRunSchema.class
        );
    }

    public static WFSpecSchema lookupWFSpec(String id, Config config)
    throws LHLookupException {

        String url = config.getAPIUrlFor(Constants.WF_SPEC_API_PATH) + "/" + id;
        return (WFSpecSchema) LookerUpper.lookupSchema(
            url, config, WFSpecSchema.class
        );
    }

    public static TaskDefSchema lookupTaskDef(
        String id, Config config
    ) throws LHLookupException {

        String url = config.getAPIUrlFor(Constants.TASK_DEF_API_PATH) + "/" + id;
        return (TaskDefSchema) LookerUpper.lookupSchema(
            url, config, TaskDefSchema.class
        );
    }

    public static ExternalEventDefSchema lookupExternalEventDef(
        String id, Config config) throws LHLookupException
    {

        String url = config.getAPIUrlFor(
            Constants.EXTERNAL_EVENT_DEF_PATH
        ) + "/" + id;

        return (ExternalEventDefSchema) LookerUpper.lookupSchema(
            url, config, ExternalEventDefSchema.class
        );
    }
}


class LookerUpper {
    public static BaseSchema lookupSchema(
        String url, Config config, Class<? extends BaseSchema> valueType
    ) throws LHLookupException {

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
            throw new LHLookupException(exn, LHLookupExceptionReason.IO_FAILURE, err);
        }

        // Check response code.
        if (response.code() == 404) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OBJECT_NOT_FOUND,
                "Could not find object at URL " + url
            );
        } else if (response.code() != 200) {
            if (responseBody == null) {
                responseBody = "";
            }
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OTHER_ERROR,
                "API Returned an error: " + String.valueOf(response.code()) + " "
                + responseBody
            );
        }

        BaseSchema out = BaseSchema.fromString(responseBody, valueType);
        if (out == null) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an unparseable response: " + responseBody
            );
        }
        out.setConfig(config);
        return out;
    }
}