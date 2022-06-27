package io.littlehorse.common.util;

import java.io.IOException;
import io.littlehorse.api.ResponseStatus;
import io.littlehorse.api.metadata.RangeQueryResponse;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.GETable;
import io.littlehorse.common.objects.metadata.WFSpec;
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
 * <<1 week later...wtf was I about to say? I left the note blank and forgot what
 *   I was gonna say>>
 */
public class LHDatabaseClient {

    public static WFSpec getWFSpecById(String idOrName, LHConfig config)
    throws LHConnectionError {
        OkHttpClient client = config.getHttpClient();
        String url = config.getAPIUrlFor("WFSpec") + "/" + idOrName;
        Request request = new Request.Builder().url(
            url
        ).build();

        try {
            Response resp = client.newCall(request).execute();
            byte[] body = resp.body().bytes();
            if (resp.code() < 300 && resp.code() >= 200) {
                return BaseSchema.fromBytes(body, WFSpec.class, config);
            } else {
                return null;
            }
        } catch(IOException exn) {
            throw new LHConnectionError(
                exn,
                String.format(
                    "Had %s error connectiong to %s: %s",
                    exn.getClass().getName(), url, exn.getMessage()
                )
            );
        } catch(LHSerdeError exn) {
            // Not possible
            return null;
        }
    }


    public static<T extends GETable> T getByNameOrId(
        String idOrName, LHConfig config, Class<T> cls
    ) throws LHConnectionError {

        LHRpcClient client = new LHRpcClient(config);
        String url = config.getAPIUrlFor(T.getAPIPath(cls)) + "/" + idOrName;
        LHRpcResponse<T> response = client.getResponse(url, cls);

        if (response.result == null) {
            // Try to look up by name.
            url = config.getAPIUrlFor(T.getSearchPath("name", idOrName, cls));

            LHRpcResponse<RangeQueryResponse> entries = client.getResponse(
                url, RangeQueryResponse.class
            );
            if (entries.status == ResponseStatus.OBJECT_NOT_FOUND) return null;

            url = config.getAPIUrlFor(T.getAPIPath(cls)) + "/";
            url += entries.result.objectIds.get(entries.result.objectIds.size() - 1);

            response = client.getResponse(url, cls);
        }
        return response.result;
    }

    public static<T extends GETable> T getById(
        String id, LHConfig config, Class<T> cls
    ) throws LHConnectionError {

        LHRpcClient client = new LHRpcClient(config);
        String url = config.getAPIUrlFor(T.getAPIPath(cls)) + "/" + id;
        LHRpcResponse<T> response = client.getResponse(url, cls);

        return response.result;
    }
}
