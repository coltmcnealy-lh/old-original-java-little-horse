package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.kafkaStreamsSerdes.TaskDefSerdes;
import little.horse.lib.schemas.TaskDefSchema;

public class TaskDefTopology {

    public static void addStuff(Topology topology, Config config) {
        TaskDefSerdes serde = new TaskDefSerdes();

        String byGuidProcessorName = "TaskDef Guid Processor";
        String byNameProcessorName = "TaskDef Name Processor";
        String sourceName = "TaskDef Metadata Events";
        String nameKeyedSink = "TaskDef Name Keyed Sink";
        String nameKeyedSource = "TaskDef Name Keyed Source";

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {serde.close();})
        );

        topology.addSource(
            sourceName,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getTaskDefTopic()
        );

        // This step does two things:
        // 1. Save the TaskDefSchema into the Guid-keyed State Store
        // 2. Produce a re-keyed record to the Name-keyed Kafka Topic
        topology.addProcessor(
            byGuidProcessorName,
            TaskDefByGuidProcessor::new,
            sourceName
        );

        // Sink the re-keyed stream into an intermediate topic
        topology.addSink(
            nameKeyedSink,
            config.getTaskDefNameKeyedTopic(),
            Serdes.String().serializer(),
            serde.serializer(),
            byGuidProcessorName
        );

        // Add a source so we can continue processing
        topology.addSource(
            nameKeyedSource,
            Serdes.String().deserializer(),
            serde.deserializer(),
            config.getTaskDefNameKeyedTopic()
        );

        topology.addProcessor(
            byNameProcessorName,
            TaskDefByNameProcessor::new,
            nameKeyedSource
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
        topology.addStateStore(guidStoreBuilder, byGuidProcessorName);
        topology.addStateStore(nameStoreBuilder, byNameProcessorName);

    }
}