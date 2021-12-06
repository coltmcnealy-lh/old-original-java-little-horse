package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.api.TaskDefProcessors.TaskDefByGuidProcessor;
import little.horse.api.TaskDefProcessors.TaskDefByNameProcessor;
import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.TaskDef.TaskDefSchema;
import little.horse.lib.TaskDef.kafkaStreamsSerdes.TaskDefSerdes;

public class TaskDefTopology {
    private Config config;

    public TaskDefTopology(Config config) {
        this.config = config;
    }

    public Topology getTopology() {
        Topology builder = new Topology();
        TaskDefSerdes serde = new TaskDefSerdes();

        String byGuidProcessorName = "TaskDef Guid Processor";
        String byNameProcessorName = "TaskDef Name Processor";
        String sourceName = "TaskDef Metadata Events";

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {serde.close();})
        );

        builder.addSource(
            sourceName,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getTaskDefTopic()
        );

        builder.addProcessor(
            byGuidProcessorName,
            TaskDefByGuidProcessor::new,
            sourceName
        );

        builder.addProcessor(
            byNameProcessorName,
            TaskDefByNameProcessor::new,
            byGuidProcessorName
        );

        StoreBuilder<KeyValueStore<String, TaskDefSchema>> guidStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.TASK_DEF_GUID_STORE),
                Serdes.String(),
                serde
        );
        StoreBuilder<KeyValueStore<String, TaskDefSchema>> nameStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.TASK_DEF_NAME_STORE),
                Serdes.String(),
                serde
        );

        // add the state store to our topology and connect it to the "Digital Twin Processor"
        builder.addStateStore(guidStoreBuilder, byGuidProcessorName);
        builder.addStateStore(nameStoreBuilder, byNameProcessorName);

        return builder;
    }
}