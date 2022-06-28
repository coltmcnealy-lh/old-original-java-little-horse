package io.littlehorse.scheduler;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.events.WFEvent;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.model.rundata.WFRun;
import io.littlehorse.common.util.Constants;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.common.util.serdes.LHSerdes;

import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

class LHTopicNameExtractor implements TopicNameExtractor<String, SchedulerOutput> {
    public String extract(String key, SchedulerOutput value, RecordContext ctx) {
        TaskScheduleRequest tsr = value.request;
        if (tsr == null) {
            // TODO: Some error handling here
        }
        return tsr.taskDefId;
    }
}


class SchedulerOutputToWFRunSerializer implements Serializer<SchedulerOutput> {
    @Override
    public byte[] serialize(String topic, SchedulerOutput thingy) {
        if (thingy == null) return null;
        WFRun run = thingy.wfRun;
        if (run == null) {
            LHUtil.logError("WARNING: Got null wfrun; prolly a bug.");
            return null;
        }
        return run.toBytes();
    }
}


class SchedulerOutputToTsrSerializer implements Serializer<SchedulerOutput> {
    @Override
    public byte[] serialize(String topic, SchedulerOutput thingy) {
        if (thingy == null) return null;
        TaskScheduleRequest tsr = thingy.request;
        if (tsr == null) {
            LHUtil.logError("WARNING: Got null tsr; prolly a bug.");
            return null;
        }
        return tsr.toBytes();
    }
}


public class SchedulerTopology {

    public static String topoSource = "WFRun Source";
    public static String runtimeProcessor = "WFRuntime";
    public static String taskQueueSink = "Task Queue Sink";
    public static String wfRunSink = "WFRun Sink";

    public static void addStuff(
            Topology topology, LHConfig config) throws LHConnectionError {
        /*
        This topology is headless—it doesn't present an API for querying (that's done
        by the LittleHorse API's MetadataTopologyBuilder<WFRun.class>). All it does is
        schedule tasks via TaskQueue's topics, and also throw the state of the WFRun
        to an output kafka topic so that it may be stored by the central API.

        The topology is NOT stateless: it needs to maintain the state of each WFRun in
        order to know how to react to every event that comes in.
        */

        LHSerdes<WFEvent> eventSerdes = new LHSerdes<>(WFEvent.class, config);
        LHSerdes<WFRun> runSerde = new LHSerdes<>(WFRun.class, config);
        LHSerdes<TaskScheduleRequest> tsrSerdes = new LHSerdes<>(
            TaskScheduleRequest.class, config
        );

        topology.addSource(
            topoSource,
            Serdes.String().deserializer(),
            eventSerdes.deserializer(),
            config.getWFRunEventTopic()
        );
        topology.addProcessor(
            runtimeProcessor,
            () -> {return new SchedulerProcessor(config);},
            topoSource
        );

        TopicNameExtractor<String, SchedulerOutput> extractor = new LHTopicNameExtractor();

        topology.addSink(
            taskQueueSink,
            extractor,
            Serdes.String().serializer(),
            new SchedulerOutputToTsrSerializer(),            
            runtimeProcessor
        );

        topology.addSink(
            wfRunSink,
            WFRun.getIdKafkaTopic(config, WFRun.class),
            Serdes.String().serializer(),
            new SchedulerOutputToWFRunSerializer(),
            runtimeProcessor
        );

        // Add state store. The only stateful processor is the Runtime Processor which
        // keeps track of the state of the WFRun to be able to schedule properly.
        StoreBuilder<KeyValueStore<String, WFRun>> wfRunStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.WF_RUN_STORE_NAME),
                Serdes.String(),
                runSerde
            );
        topology.addStateStore(wfRunStoreBuilder, runtimeProcessor);

        StoreBuilder<KeyValueStore<String, Bytes>> timerStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.TIMER_STORE_NAME),
                Serdes.String(),
                Serdes.Bytes()
            );
        topology.addStateStore(timerStoreBuilder, runtimeProcessor);

        // cleanup.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            eventSerdes.close();
            tsrSerdes.close();
            runSerde.close();
        }));
    }
}
