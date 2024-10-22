package little.horse.common.util;

import little.horse.api.ResponseStatus;
import little.horse.api.metadata.IndexEntryCollection;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.GETable;


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
        String idOrName, DepInjContext config, Class<T> cls
    ) throws LHConnectionError {

        LHRpcCLient client = new LHRpcCLient(config);
        String url = config.getAPIUrlFor(T.getAPIPath(cls)) + "/" + idOrName;
        LHRpcResponse<T> response = client.getResponse(url, cls);

        if (response.result == null) {
            // Try to look up by name.
            url = config.getAPIUrlFor(T.getAliasPath(cls)) + "/name/" + idOrName;

            LHRpcResponse<IndexEntryCollection> entries = client.getResponse(
                url, IndexEntryCollection.class
            );
            if (entries.status == ResponseStatus.OBJECT_NOT_FOUND) return null;
            
            url = config.getAPIUrlFor(T.getAPIPath(cls)) + "/";
            url += entries.result.getLatestEntry().objectId;

            response = client.getResponse(url, cls);
        }
        return response.result;
    }

    public static<T extends GETable> T getById(
        String id, DepInjContext config, Class<T> cls
    ) throws LHConnectionError {

        LHRpcCLient client = new LHRpcCLient(config);
        String url = config.getAPIUrlFor(T.getAPIPath(cls)) + "/" + id;
        LHRpcResponse<T> response = client.getResponse(url, cls);

        return response.result;
    }
}
