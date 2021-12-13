package little.horse.api;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.lib.TaskDefSchema;
import little.horse.lib.WFRunSchema;
import little.horse.lib.WFSpecSchema;

public class APIStreamsContext {
    private String wfSpecNameStoreName;
    private String wfSpecGuidStoreName;
    private KafkaStreams streams;

    private String taskDefNameStoreName;
    private String taskDefGuidStoreName;
    private String wfRunStoreName;
    
    public String getTaskDefNameStoreName() {
        return taskDefNameStoreName;
    }

    public String getWFRunStoreName() {
        return wfRunStoreName;
    }

    public void setWFRunStoreName(String name) {
        this.wfRunStoreName = name;
    }

    public void setTaskDefNameStoreName(String taskDefNameStoreName) {
        this.taskDefNameStoreName = taskDefNameStoreName;
    }

    public String getTaskDefGuidStoreName() {
        return taskDefGuidStoreName;
    }

    public void setTaskDefGuidStoreName(String taskDefGuidStoreName) {
        this.taskDefGuidStoreName = taskDefGuidStoreName;
    }

    public APIStreamsContext(KafkaStreams streams) {
        this.streams = streams;
    }

    public void setWFSpecNameStoreName(String name) {
        this.wfSpecNameStoreName = name;
    }

    public void setWFSpecGuidStoreName(String name) {
        this.wfSpecGuidStoreName = name;
    }

    public ReadOnlyKeyValueStore<String, WFSpecSchema> getWFSpecNameStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfSpecNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, WFSpecSchema> getWFSpecGuidStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfSpecGuidStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, TaskDefSchema> getTaskDefNameStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.taskDefNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, TaskDefSchema> getTaskDefGuidStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.taskDefGuidStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, WFRunSchema> getWFRunStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfRunStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }
}
