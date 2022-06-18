package io.littlehorse.common.util;

import io.littlehorse.api.ResponseStatus;
import io.littlehorse.api.metadata.RangeQueryResponse;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.metadata.GETable;


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
