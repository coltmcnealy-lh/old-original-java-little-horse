package little.horse.api.util;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.objects.metadata.ExternalEventDefSchema;
import little.horse.common.objects.metadata.TaskDefSchema;
import little.horse.common.objects.metadata.WFSpecSchema;
import little.horse.common.objects.rundata.WFRunSchema;

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

    public ReadOnlyKeyValueStore<String, ExternalEventDefSchema> getExternalEventDefNameStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.externalEventDefNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }

    public ReadOnlyKeyValueStore<String, ExternalEventDefSchema> getExternalEventDefGuidStore() {
        return streams.store(
            StoreQueryParameters.fromNameAndType(
                this.externalEventDefGuidStoreName,
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
