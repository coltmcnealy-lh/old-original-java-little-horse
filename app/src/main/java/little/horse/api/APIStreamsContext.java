package little.horse.api;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.lib.TaskDef.TaskDefSchema;
import little.horse.lib.WFSpec.WFSpecSchema;

public class APIStreamsContext {
    private String wfSpecStoreName;
    private KafkaStreams wfSpecStreams;
    private KafkaStreams taskDefStreams;

    private String taskDefNameStoreName;
    private String taskDefGuidStoreName;
    
    public String getTaskDefNameStoreName() {
        return taskDefNameStoreName;
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

    public APIStreamsContext(KafkaStreams wfSpecStreams, KafkaStreams taskDefStreams) {
        this.wfSpecStreams = wfSpecStreams;
        this.taskDefStreams = taskDefStreams;
    }

    public void setWFSpecStoreName(String name) {
        this.wfSpecStoreName = name;
    }

    public ReadOnlyKeyValueStore<String, WFSpecSchema> getWFSpecStore() {
        return wfSpecStreams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfSpecStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, TaskDefSchema> getTaskDefNameStore() {
        return wfSpecStreams.store(
            StoreQueryParameters.fromNameAndType(
                this.taskDefNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, TaskDefSchema> getTaskDefGuidStore() {
        return taskDefStreams.store(
            StoreQueryParameters.fromNameAndType(
                this.taskDefGuidStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }
}
