package little.horse.api.util;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.metadata.ExternalEventDef;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;

public class APIStreamsContext {
    private String wfSpecNameStoreName;
    private String wfSpecGuidStoreName;
    private KafkaStreams streams;

    private String taskDefNameStoreName;
    private String taskDefGuidStoreName;
    private String externalEventDefGuidStoreName;
    private String externalEventDefNameStoreName;
    private String wfRunStoreName;
    
    public String getTaskDefNameStoreName() {
        return taskDefNameStoreName;
    }

    public String getExternalEventDefNameStoreName() {
        return externalEventDefNameStoreName;
    }

    public String getExternalEventDefGuidStoreName() {
        return externalEventDefGuidStoreName;
    }

    public void setExternalEventDefNameStoreName(String name) {
        this.externalEventDefNameStoreName = name;
    }

    public void setExternalEventDefGuidStoreName(String name) {
        this.externalEventDefGuidStoreName = name;
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

    public ReadOnlyKeyValueStore<String, WFSpec> getWFSpecNameStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfSpecNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, WFSpec> getWFSpecGuidStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfSpecGuidStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, TaskDef> getTaskDefNameStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.taskDefNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, TaskDef> getTaskDefGuidStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.taskDefGuidStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, ExternalEventDef> getExternalEventDefNameStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.externalEventDefNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, ExternalEventDef> getExternalEventDefGuidStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.externalEventDefGuidStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, WFRun> getWFRunStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.wfRunStoreName,
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
