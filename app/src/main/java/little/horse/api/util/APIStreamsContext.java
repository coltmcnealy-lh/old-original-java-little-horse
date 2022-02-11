package little.horse.api.util;

import java.util.Set;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.api.metadata.CoreMetadataEntry;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.Constants;
import little.horse.common.util.LHRpcCLient;
import little.horse.common.util.LHRpcResponse;
import little.horse.common.util.LHUtil;

public class APIStreamsContext<T extends CoreMetadata> {
    private KafkaStreams streams;
    private HostInfo thisHost;
    private Class<T> cls;
    private Config config;

    public APIStreamsContext(KafkaStreams streams, Class<T> cls, Config config) {
        this.streams = streams;
        this.thisHost = config.getHostInfo();
        this.cls = cls;
        this.config = config;
    }

    private ReadOnlyKeyValueStore<String, Bytes> getStore(String storeName) {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                storeName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public T getTFromId(String id, boolean forceLocal) throws LHConnectionError {
        Bytes objBytes = queryStoreBytes(id, T.getIdStoreName(), forceLocal);

        try {
            CoreMetadataEntry entry = BaseSchema.fromBytes(
                objBytes.get(),
                CoreMetadataEntry.class,
                config
            );
            return entry == null ? null : BaseSchema.fromString(
                entry.content, cls, config
            );

        } catch (LHSerdeError exn) {
            throw new RuntimeException(
                "Yack, how did invalid json get in there?"
            );
        }
    }

    public T getTFromAlias(String aliasKey, String aliasValue, boolean forceLocal)
    throws LHConnectionError {
        return null;
    }

    public void waitForProcessing(
        String key, RecordMetadata record, boolean forceLocal
    ) {

    }

    private Bytes queryStoreBytes(
        String storeName, String storeKey, boolean forceLocal
    ) throws LHConnectionError {
        KeyQueryMetadata metadata = streams.queryMetadataForKey(
            storeName,
            storeKey,
            Serdes.String().serializer()
        );

        if (forceLocal || metadata.activeHost().equals(thisHost)) {
            return getStore(storeName).get(storeKey);

        } else {

            try {
                return queryRemote(storeKey, storeName, metadata.activeHost());

            } catch (LHConnectionError exn) {
                LHUtil.logError(
                    "Trying to read from stale replica: ", exn.getMessage()
                );
                return queryStandbys(storeKey, storeName, metadata);

            }
        }
    }

    private Bytes queryRemote(
        String storeKey, String storeName, HostInfo hostInfo
    ) throws LHConnectionError {
        String remoteHost = hostInfo.host();
        int remotePort = hostInfo.port();
        String url = String.format(
            "http://%s:%d%s/%s/%s",
            remoteHost,
            remotePort,
            T.getAPIPath(),
            Constants.FORCE_LOCAL,
            storeKey
        );

        LHRpcCLient client = new LHRpcCLient(config);
        LHRpcResponse<T> response = client.get(url, cls);

        switch (response.status) {
            case OK:
                return new Bytes(response.result.toBytes());
            case OBJECT_NOT_FOUND:
                return null;
            case INTERNAL_ERROR:
                throw new LHConnectionError(
                    null,
                    response.message
                );
            case VALIDATION_ERROR:
                throw new RuntimeException(
                    "This shouldn't be possible on a GET request."
                );
            default:
                throw new RuntimeException("unknown response status.");
        }

    }

    private Bytes queryStandbys(
        String storeKey, String storeName, KeyQueryMetadata metadata
    ) throws LHConnectionError {
        Set<HostInfo> standbys = metadata.standbyHosts();
        for (HostInfo host: standbys) {
            if (host.equals(metadata.activeHost())) continue; // already tried.
            try {
                return queryRemote(storeKey, storeName, host);
            } catch(LHConnectionError exn) {
                // Just swallow it and throw later.
            }
        }
        throw new LHConnectionError(
            null,
            "Neither active nor standby hosts returned valid response."
        );
    }

}