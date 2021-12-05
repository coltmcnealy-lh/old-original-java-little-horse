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

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TaskDefSchema> taskDefEvents = builder.stream(
            config.getTaskDefTopic(),
            Consumed.with(Serdes.String(), new TaskDefSerdes())
        );

        KStream<String, TaskDefSchema> taskDefEventsNameKeyed = taskDefEvents.selectKey(
            ((k, v) -> v.name)
        );
        String intermediateName = config.getTaskDefTopic() + "__intermediate";
        String intermediateGuid = intermediateName + "2";
        taskDefEventsNameKeyed.to(intermediateName);
        taskDefEvents.to(intermediateGuid);

        this.taskDefNameTable = builder.globalTable(
            intermediateName,
            Materialized.<String, TaskDefSchema, KeyValueStore<Bytes, byte[]>>
                as("wf-spec-name")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TaskDefSerdes())
        );

        this.taskDefGuidTable = builder.globalTable(
            intermediateGuid,
            Materialized.<String, TaskDefSchema, KeyValueStore<Bytes, byte[]>>
                as("wf-spec-guid")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TaskDefSerdes())
        );

        return builder.build();
    }

    public String getGuidStoreName() {
        return taskDefGuidTable.queryableStoreName();
    }

    public String getNameStoreName() {
        return taskDefNameTable.queryableStoreName();
    }
}