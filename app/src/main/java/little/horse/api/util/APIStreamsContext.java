package little.horse.api.util;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.common.objects.metadata.CoreMetadata;

public class APIStreamsContext {
    private KafkaStreams streams;

    public APIStreamsContext(KafkaStreams streams) {
        this.streams = streams;
    }

    public <T extends CoreMetadata> ReadOnlyKeyValueStore<String, T> getIdStore(
        Class<T> cls
    ) {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                T.getStoreName(),
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public <T extends CoreMetadata> ReadOnlyKeyValueStore<String, T> getNameStore(
        Class<T> cls
    ) {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                T.getNameStoreName(),
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public <T extends CoreMetadata> void waitForProcessing(
        RecordMetadata record, Class<T> cls
    ) {
        // TODO: wait until the record has been processed.
        // Can start with a simple polling implementation, but later on
        // we will replace it with actual proper thread stuff.
        try {
            Thread.sleep(100);
        } catch (Exception exn) {}
    }
}
