/**
 * This, along with CoreMetadataAPI.java, is **almost** an implementation of a
 * full-blown database with indexes (the alias thing). It's not quite as robust as SQL
 * but it hopefully gets the job done.
 */

package little.horse.api.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

import little.horse.api.OffsetInfoCollection;
import little.horse.api.metadata.AliasEntryCollection;
import little.horse.api.metadata.AliasIdentifier;
import little.horse.api.metadata.CoreMetadataEntry;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.Constants;
import little.horse.common.util.LHRpcCLient;
import little.horse.common.util.LHRpcRawResponse;
import little.horse.common.util.LHUtil;

public class APIStreamsContext<T extends CoreMetadata> {
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
            CoreMetadataEntry entry = objBytes == null ? null : BaseSchema.fromBytes(
                objBytes.get(),
                CoreMetadataEntry.class,
                config
            );
            return entry == null ? null : BaseSchema.fromString(
                entry.content, cls, config
            );
            // return objBytes == null ?
            //     null : BaseSchema.fromBytes(
            //         objBytes.get(), CoreMetadataEntry.class, config
            //     );

        } catch (LHSerdeError exn) {
            throw new RuntimeException(
                "Yack, how did invalid json get in there?"
            );
        }
    }

    public ArrayList<String> getAllIds(boolean forceLocal) throws LHConnectionError {
        ReadOnlyKeyValueStore<String, Bytes> store = getStore(T.getIdStoreName(
            cls
        ));

        ArrayList<String> out = new ArrayList<>();

        if (forceLocal) {
            KeyValueIterator<String, Bytes> allIter = null;
            allIter = store.all();
            try {
                while (allIter.hasNext()) {
                    KeyValue<String, Bytes> kv = allIter.next();
                    if (!kv.key.equals(Constants.LATEST_OFFSET_ROCKSDB_KEY)) {
                        out.add(kv.key);
                    }
                }
            } finally {
                if (allIter != null) allIter.close();;
            }

        } else {
            for (StreamsMetadata meta: streams.metadataForAllStreamsClients()) {
                String host = meta.host();
                int port = meta.port();
                String url = String.format(
                    "http://%s:%d%s?%s=true",
                    host,
                    port,
                    T.getAllAPIPath(cls),
                    Constants.FORCE_LOCAL
                );
                byte[] rawResponse = new LHRpcCLient(config).getResponse(url);
                try {
                    List<?> rawList = LHUtil.getObjectMapper(config).readValue(
                        rawResponse, List.class
                    );
                    for (Object thing: rawList) {
                        String id = String.class.cast(thing);
                        out.add(id);
                    }
                } catch (IOException exn) {
                    throw new LHConnectionError(exn, "got bad remote response");
                }
            }
        }

        return out;
    }

    public Long getOffsetIDStore(String id, boolean forceLocal, String apiPath)
    throws LHConnectionError {
        // Need the ID in order to figure out the partition thing.
        Bytes objBytes = queryStoreBytes(
            T.getIdStoreName(cls), Constants.LATEST_OFFSET_ROCKSDB_KEY, forceLocal,
            apiPath, id
        );
        if (objBytes == null) return null;

        return Long.valueOf(String.valueOf(objBytes.get()));
    }

    public AliasEntryCollection getTFromAlias(
        String aliasKey, String aliasValue, boolean forceLocal
    ) throws LHConnectionError {
        String apiPath = T.getAliasSetPath(aliasKey, aliasValue, cls);
        AliasIdentifier entryID = new AliasIdentifier(aliasKey, aliasValue);

        Bytes aliasEntryCollectionBytes = queryStoreBytes(
            T.getAliasStoreName(cls), entryID.getStoreKey(), forceLocal, apiPath
        );

        try {
            if (aliasEntryCollectionBytes == null) return null;

            return BaseSchema.fromBytes(
                aliasEntryCollectionBytes.get(), AliasEntryCollection.class, config
            );
        } catch(LHSerdeError exn) {
            throw new RuntimeException("Somehow we got invalid crap in rocksdb");
        }
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
            while (true) {
                OffsetInfoCollection infos;
                Bytes b = getStore(T.getIdStoreName(cls)).get(
                    Constants.LATEST_OFFSET_ROCKSDB_KEY
                );

                try {
                    infos = (b != null) ? BaseSchema.fromBytes(
                        b.get(), OffsetInfoCollection.class, config
                    ) : null;
                } catch(LHSerdeError exn) {
                    exn.printStackTrace();
                    throw new RuntimeException("this should be impossible");
                }
                if (infos != null) {
                    String mapKey = T.getIdKafkaTopic(config, cls) + partition;
                    if (infos.partitionMap.get(mapKey).offset >= offset) {
                        break;
                    }
                }

                try {
                    Thread.sleep(100);
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
                "Calling wait externally:", url
            );
            new LHRpcCLient(config).getResponse(url, cls);
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
            "http://%s:%d/storeBytes/%s/%s",
            remoteHost,
            remotePort,
            storeName,
            storeKey
        );
        LHRpcCLient client = new LHRpcCLient(config);
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
                System.out.println("Failed on host\n\n\n\n" + host + "\n\n\n"); 
                exn.printStackTrace();
            }
        }
        LHUtil.log("\n\n\n", metadata.standbyHosts(), "\n\n\n");
        throw new LHConnectionError(
            null,
            "Neither active nor standby hosts returned valid response."
        );
    }

}