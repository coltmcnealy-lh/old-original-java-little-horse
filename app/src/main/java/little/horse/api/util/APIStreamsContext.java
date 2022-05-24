/**
 * This, along with GETApi.java, is **almost** an implementation of a
 * full-blown database with indexes (the alias thing). It's not quite as robust as SQL
 * but it hopefully gets the job done.
 */

package little.horse.api.util;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.api.OffsetInfo;
import little.horse.api.metadata.IndexEntry;
import little.horse.api.metadata.RangeQueryResponse;
import little.horse.api.metadata.ResourceDbEntry;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.GETable;
import little.horse.common.util.LHRpcClient;
import little.horse.common.util.LHRpcRawResponse;
import little.horse.common.util.LHRpcResponse;
import little.horse.common.util.LHUtil;

public class APIStreamsContext<T extends GETable> {
    private KafkaStreams streams;
    private HostInfo thisHost;
    private Class<T> cls;
    private DepInjContext config;

    public APIStreamsContext(KafkaStreams streams, Class<T> cls, DepInjContext config) {
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

    public T getTFromId(String id, boolean forceLocal)
    throws LHConnectionError {
        Bytes objBytes = queryStoreBytes(
            T.getIdStoreName(cls), id, forceLocal, T.getAPIPath(id, cls)
        );

        try {
            ResourceDbEntry entry = objBytes == null ? null : BaseSchema.fromBytes(
                objBytes.get(),
                ResourceDbEntry.class,
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

    private static String getFirstKey() {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            out.append(0x00);
        }
        return out.toString();
    }

    private static String getLastKey() {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            out.append(0xff);
        }
        return out.toString();
    }

    public RangeQueryResponse search(
        String key, String value, String token, int limit
    ) throws LHConnectionError {

        String start = key + "__" + value;
        String end = start + getLastKey();
        return iterBetweenKeys(start, end, limit, token, false);
    }

    public RangeQueryResponse list(
        String token, int limit
    ) throws LHConnectionError {
        String start = "created__";
        String end = start + getLastKey();
        return iterBetweenKeys(start, end, limit, token, false);
    }

    private RangeQueryResponse iterBetweenKeysLocal(
        String start, String end, int limit, Map<Integer, String> bookmarkMap
    ) {
        if (end == null) end = getLastKey();

        RangeQueryResponse out = new RangeQueryResponse();
        ReadOnlyKeyValueStore<String, Bytes> store = getStore(
            T.getIndexStoreName(cls)
        );
        KeyValueIterator<String, Bytes> iter = store.range(start, end);

        try {
            while (iter.hasNext() && out.objectIds.size() < limit) {
                KeyValue<String, Bytes> kv = iter.next();
                IndexEntry lastEntry = BaseSchema.fromBytes(
                    kv.value.get(), IndexEntry.class, config
                );

                String smallestKey = bookmarkMap.get(lastEntry.partition);
                if (smallestKey != null &&
                    smallestKey.compareTo(lastEntry.objectId) <= 0
                ) {
                    LHUtil.log(
                        "Partition", lastEntry.partition, "has seen ",
                        smallestKey, "So we skip", lastEntry.objectId
                    );
                    continue;
                }

                out.objectIds.add(kv.key);
                out.partitionBookmarks.put(lastEntry.partition, lastEntry.objectId);
            }
        } catch (LHSerdeError exn) {
            // Nothing to do since this isn't possible;
            throw new RuntimeException(exn);
        } finally {
            iter.close();
        }

        return out;
    }

    public RangeQueryResponse iterBetweenKeys(
        String start, String end, int limit, String token, boolean forceLocal
    ) throws LHConnectionError {

        Map<Integer, String> bookmarkMap = RangeQueryResponse.tokenToBookmarkMap(
            token, config
        );
        if (end == null) {
            end = getLastKey();
        }
        if (start == null) {
            if (RangeQueryResponse.lowestKeySeen(bookmarkMap) != null) {
                start = RangeQueryResponse.lowestKeySeen(bookmarkMap);
            } else {
                start = getFirstKey();
            }
        }

        if (forceLocal) {
            return iterBetweenKeysLocal(
                start, end, limit,
                RangeQueryResponse.tokenToBookmarkMap(token, config)
            );
        }

        RangeQueryResponse out = new RangeQueryResponse();
        for (StreamsMetadata meta: streams.metadataForAllStreamsClients()) {
            String host = meta.host();
            int port = meta.port();
            String url = String.format(
                "%s:%d%s",
                host,
                port,
                T.getInternalIterAPIPath(
                    start, end, token, String.valueOf(limit), cls
                )
            );

            LHRpcResponse<RangeQueryResponse> resp = new LHRpcClient(
                config
            ).getResponse(url, RangeQueryResponse.class);

            if (resp.result != null) {
                out = out.add(resp.result);
            }
        }
        return out;
    }

    public void waitForProcessing(
        String key, long offset, int partition, boolean forceLocal, String apiPath
    ) throws LHConnectionError {
        KeyQueryMetadata metadata = streams.queryMetadataForKey(
            T.getIdStoreName(cls),
            key,
            Serdes.String().serializer()
        );

        if (forceLocal || metadata.activeHost().equals(thisHost)) {
            String offsetInfoKey = OffsetInfo.getKey(
                T.getIdKafkaTopic(config, cls),
                partition
            );
            // TODO: Need to add a timeout to this.
            while (true) {
                Bytes b = getStore(T.getIdStoreName(cls)).get(
                    offsetInfoKey
                );

                if (b != null) {
                    OffsetInfo info;
                    try {
                        info = BaseSchema.fromBytes(b.get(), OffsetInfo.class, config);
                    } catch(LHSerdeError exn) {
                        exn.printStackTrace();
                        throw new RuntimeException("this should be impossible");
                    }
                    if (info.offset >= offset) {
                        break;
                    }
                }

                try {
                    Thread.sleep(50);
                } catch(Exception exn) {}
            }
        } else {
            // No need to query standby's, because if the leader is down, well, then
            // there's gonna be no progress made until the leader comes back up, so we
            // just say ":shrug: the leader's down, dunno."
            
            // In the future, if Colty weren't so lazy, maybe we could query the
            // standby to see if the info had already propagated. But that's not in
            // the time budget for now.
            HostInfo host = metadata.activeHost();
            String newApiPath = forceLocal ? apiPath : apiPath + "?forceLocal=true";
            String url = String.format(
                "http://%s:%s%s", host.host(), host.port(), newApiPath
            );
            LHUtil.log(
                "Calling wait externally to", url, "from", thisHost.host()
            );
            new LHRpcClient(config).getResponse(url, cls);
        }
    }

    private Bytes queryStoreBytes(
        String storeName, String storeKey, boolean forceLocal, String apiPath
    ) throws LHConnectionError {
        // Use the store key as the partition key.
        return queryStoreBytes(storeName, storeKey, forceLocal, apiPath, storeKey);
    }

    public void dumpStore(String storeName) {
        KeyValueIterator<String, Bytes> it = getStore(storeName).all();

        System.out.println("\n\n\n\n\n\n**********************");
        while (it.hasNext()) {
            KeyValue<String, Bytes> kvp = it.next();
            System.out.println(kvp.key + "\t\t" + new String(kvp.value.get()));
        }
        System.out.println("**********************\n\n\n\n\n\n");
    }

    private Bytes queryStoreBytes(
        String storeName, String storeKey, boolean forceLocal,
        String apiPath, String partitionKey
    ) throws LHConnectionError {
        KeyQueryMetadata metadata = streams.queryMetadataForKey(
            storeName,
            partitionKey,
            Serdes.String().serializer()
        );

        if (forceLocal || metadata.activeHost().equals(thisHost)) {
            return getStore(storeName).get(storeKey);

        } else {
            try {
                return queryRemote(
                    storeKey, storeName, metadata.activeHost()
                );

            } catch (LHConnectionError exn) {
                exn.printStackTrace();
                LHUtil.logError(
                    "Trying to read from stale replica: ", exn.getMessage()
                );
                return queryStandbys(storeKey, storeName, metadata, apiPath);

            }
        }
    }

    private Bytes queryRemote(
        String storeKey, String storeName, HostInfo hostInfo
    ) throws LHConnectionError {
        String remoteHost = hostInfo.host();
        int remotePort = hostInfo.port();
        String url = String.format(
            "http://%s:%d/internal/storeBytes/%s/%s",
            remoteHost,
            remotePort,
            storeName,
            storeKey
        );
        LHRpcClient client = new LHRpcClient(config);
        LHRpcRawResponse response = client.getRawResponse(url);

        switch (response.status) {
            case OK:
                byte [] out = response.result == null ?
                    null: response.result.getBytes();
                return new Bytes(out);
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
        String storeKey, String storeName, KeyQueryMetadata metadata, String apiPath
    ) throws LHConnectionError {
        Set<HostInfo> standbys = metadata.standbyHosts();
        for (HostInfo host: standbys) {
            if (host.equals(metadata.activeHost())) continue; // already tried.
            try {
                return queryRemote(storeKey, storeName, host);
            } catch(LHConnectionError exn) {
                // Just swallow it and throw later.
                LHUtil.logError("Failed on host" + host + "\n\n\n");
                exn.printStackTrace();
            }
        }
        throw new LHConnectionError(
            null,
            "Neither active nor standby hosts returned valid response."
        );
    }

}