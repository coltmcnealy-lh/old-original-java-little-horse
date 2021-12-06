package little.horse.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import little.horse.lib.Config;
import little.horse.lib.TaskDef.TaskDefSchema;
import little.horse.lib.TaskDef.kafkaStreamsSerdes.TaskDefSerdes;

public class TaskDefTopology {
    private Config config;
    private GlobalKTable<String, TaskDefSchema> taskDefGuidTable;
    private GlobalKTable<String, TaskDefSchema> taskDefNameTable;

    public TaskDefTopology(Config config) {
        this.config = config;
    }

    public Topology buildIndex() {
        StreamsBuilder builder = new StreamsBuilder();
        String intermediateTopicByName = config.getTaskDefTopic() + "__intermediate_name";

        this.taskDefNameTable = builder.globalTable(
            intermediateTopicByName,
            Materialized.<String, TaskDefSchema, KeyValueStore<Bytes, byte[]>>
                as("wf-spec-asdfasdf")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TaskDefSerdes())
        );

        return builder.build();
    }

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TaskDefSchema> taskDefEvents = builder.stream(
            config.getTaskDefTopic(), // 'foo' in the question above
            Consumed.with(Serdes.String(), new TaskDefSerdes())
        );

        KStream<String, TaskDefSchema> taskDefEventsNameKeyed = taskDefEvents.selectKey(
            ((k, v) -> v.name)
        );
        String intermediateTopicByName = "wf-spec-asdfasdf"; //config.getTaskDefTopic() + "__intermediate_name";
        String intermediateTopicByGuid = config.getTaskDefTopic() + "__intermediate_guid";

        taskDefEventsNameKeyed.to(intermediateTopicByName);
        taskDefEvents.to(intermediateTopicByGuid);

        this.taskDefGuidTable = builder.globalTable(
            intermediateTopicByGuid,
            Materialized.<String, TaskDefSchema, KeyValueStore<Bytes, byte[]>>
                as("wf-spec-guid")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TaskDefSerdes())
        );

        return builder.build();
    }

    public String getGuidStoreName() {
        return (taskDefGuidTable == null) ? null : taskDefGuidTable.queryableStoreName();
    }

    public String getNameStoreName() {
        return taskDefNameTable.queryableStoreName();
    }
}