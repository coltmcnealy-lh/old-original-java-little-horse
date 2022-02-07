package little.horse.common.util;

import java.io.IOException;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHLookupExceptionReason;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.LHSerdeError;
import little.horse.common.objects.metadata.CoreMetadata;
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

    public static<T extends CoreMetadata> T lookupMeta(
        String guid, Config config, Class<T> cls
    ) throws LHConnectionError {
        String url = config.getAPIUrlFor(T.getAPIPath()) + "/" + guid;

        LHUtil.logBack(
            1, "Making call to URL", url, "to look up", cls.getName()
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

        try {
            return BaseSchema.fromString(
                responseBody, cls, config, false
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            // That means the response we got didn't match the proper schema. Since it
            // "should" be "impossible", it's likely that the API is orzdashed.
            throw new LHConnectionError(
                null,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an unparseable response: " + responseBody
            );
        }
    }


}
