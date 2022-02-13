package little.horse.common.util;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.CoreMetadata;


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

        LHRpcCLient client = new LHRpcCLient(config);
        String url = config.getAPIUrlFor(T.getAPIPath()) + "/" + guid;
        LHRpcResponse<T> response = client.getResponse(url, cls);

        return response.result;
    }
}
