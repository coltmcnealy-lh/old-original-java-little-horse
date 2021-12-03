package little.horse.api;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.lib.WFSpec.WFSpecSchema;

public class APIStreamsContext {
    private String wfSpecStoreName;
    private KafkaStreams streams;
    
    public APIStreamsContext(KafkaStreams streams) {
        this.streams = streams;
    }

    public void setWFSpecStoreName(String name) {
        this.wfSpecStoreName = name;
    }

    public ReadOnlyKeyValueStore<String, WFSpecSchema> getWFSpecStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfSpecStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }
}
