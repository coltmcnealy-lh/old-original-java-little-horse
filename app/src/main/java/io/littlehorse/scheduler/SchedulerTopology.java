package io.littlehorse.scheduler;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.util.Constants;
import io.littlehorse.scheduler.serdes.WFRunEventDeserializer;

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

        WFRunEventDeserializer evtDeser = new WFRunEventDeserializer();

        topology.addSource(
            topoSource,
            Serdes.String().deserializer(),
            evtDeser,
            Constants.WF_RUN_EVENT_TOPIC
        );

        topology.addProcessor(
            runtimeProcessor,
            () -> {return new SchedulerProcessor();},
            topoSource
        );
    }
}
