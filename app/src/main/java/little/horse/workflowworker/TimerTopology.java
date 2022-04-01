package little.horse.workflowworker;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import little.horse.common.DepInjContext;
import little.horse.common.events.WFEvent;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.objects.rundata.WFRun;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;
import little.horse.common.util.serdes.LHSerdes;

public class TimerTopology {

    public static void addStuff(
        Topology topology, DepInjContext config, WFSpec wfSpec
    ) throws LHConnectionError {
        /*
        This topology is headless—it doesn't present an API for querying (that's done
        by the LittleHorse API's MetadataTopologyBuilder<WFRun.class>). All it does is
        schedule tasks via TaskQueue's topics, and also throw the state of the WFRun
        to an output kafka topic so that it may be stored by the central API.

        The topology is NOT stateless: it needs to maintain the state of each WFRun in
        order to know how to react to every event that comes in.

        This is slightly wasteful: the data is stored in the WF Worker's RocksDB, the
        output topic, then the API's state store, then the API's state store's
        changelog topic (since we don't have the ability to optimize like kSQL does 
        by using the same topic as the changelog for both stores).

        However, it brings a few benefits:
        1. if we queried the WF Workers directly, we would be adding another network
           hop (from the LH API to the workflow worker, and then possibly to another
           partition on the workflow worker).
        2. When we undeploy an old workflow, we remove the workflow worker. Then we'd
           lose the ability to query those old WFRun's unless we implement some
           third-party indexing store, which I don't wanna do yet.
        3. The workflow worker would have to be responsible for alias indexing.

        Some questions though:
        - What happens if two WFRun's need to coordinate shared variables?
        - Is there any reason why having all of the alias'ing done in the LH API might
          cause some sort of problem or orzdash?
        */

        String topoSource = "WFRun Source";
        String runtimeProcessor = "WFRuntime";
        LHSerdes<WFEvent> eventSerdes = new LHSerdes<>(WFEvent.class, config);
        LHSerdes<WFRun> runSerde = new LHSerdes<>(WFRun.class, config);
        LHSerdes<TaskScheduleRequest> tsrSerdes = new LHSerdes<>(
            TaskScheduleRequest.class, config
        );

        topology.addSource(
            topoSource,
            Serdes.String().deserializer(),
            eventSerdes.deserializer(),
            wfSpec.getEventTopic()
        );
        topology.addProcessor(
            runtimeProcessor,
            () -> {return new SchedulerProcessor(config, wfSpec);},
            topoSource
        );

        /*
        The runtimeProcessor forwards down the context a bunch of CoordinatorOutput
        objects. For each WFEvent, we get:

        - Exactly one CoordinatorOutput where the WFRun is not null. The WFRun then
          gets forwarded to the WFRun Output Topic so that it can be collected by
          the LH API.
        - Zero or more CoordinatorOutput's where the TaskScheduleRequest is not null.
          The desired end state is that each of the TSR's get forwarded to the kafka
          topic for the associated TaskQueue. How does that work? Well, for every
          TaskQueue touched by the workflow, we add a processor that filters the
          TSR's by taskQueue name, and if there's a double-swipe-right (a match), then
          the processor sends the TSR to the Task Queue's appropriate topic.
        */

        // First, the TaskQueue's
        for (TaskDef tq: wfSpec.getAllTaskDefs()) {
            String procName = "Filter Processor " + tq.name;

            LHUtil.log("Task queue", tq.getId());

            topology.addProcessor(
                procName,
                () -> {return new SchedulerFanoutProcessor(tq);},
                runtimeProcessor
            );

            // Fire it off to the actual kafka topic.
            topology.addSink(
                procName + " queue sink",
                tq.getKafkaTopic(),
                Serdes.String().serializer(),
                tsrSerdes.serializer(),
                procName
            );
        }

        // Now, forward the WFRun's on to another topic for processing by the API.
        String wfRunSink = "wfRun Sink Processor";
        topology.addProcessor(
            wfRunSink, () -> {return new SchedulerWFRunSinkProcessor();}, runtimeProcessor
        );

        topology.addSink(
            "actual sink for the wfrun",
            // Here, we basically say that all WFRun's share the same topic. That
            // is fine for now but in the future it'll make it impossible to scale
            // up the number of partitions for the WFRun collector. We want to make
            // this 
            WFRun.getIdKafkaTopic(config, WFRun.class),
            Serdes.String().serializer(),
            runSerde.serializer(),
            wfRunSink
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

        StoreBuilder<KeyValueStore<Long, Bytes>> timerStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.TIMER_STORE_NAME),
                Serdes.Long(),
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
