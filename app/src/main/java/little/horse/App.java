/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package little.horse;

import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import little.horse.api.APIStreamsContext;
import little.horse.api.TaskDefTopology;
import little.horse.api.WFSpecDeployer;
import little.horse.api.WFSpecTopology;
import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.NullWFEventActor;
import little.horse.lib.TaskDaemonEventActor;
import little.horse.lib.WFEventProcessorActor;
import little.horse.lib.WFRunTopology;
import little.horse.lib.kafkaStreamsSerdes.LHDeserializer;
import little.horse.lib.objects.TaskDef;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.TaskDefSchema;


class FrontendAPIApp {
    private static void createKafkaTopics(Config config) {
        int partitions = 1;
        short replicationFactor = 1;

        String[] topics = {
            config.getWFSpecActionsTopic(),
            config.getWFSpecTopic(),
            config.getWFSpecIntermediateTopic(),
            config.getTaskDefNameKeyedTopic(),
            config.getTaskDefTopic(),
            config.getWFSpecNameKeyedTopic()
        };
        for (String topicName : topics) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            config.createKafkaTopic(newTopic);
        }
    }

    /**
     * Does three things:
     * 1. Sets up a KafkaStreams topology for processing WFSpec, TaskDef, and WFRun updates.
     * 2. Sets up a LittleHorseAPI to respond to metadata control requests.
     * 3. Sets up a listener for new WFSpecs that deploys them to kubernetes (if necessary).
     */
    public static void run() {
        Config config = null;
        config = new Config();

        FrontendAPIApp.createKafkaTopics(config);
        Topology topology = new Topology();

        TaskDefTopology.addStuff(topology, config);
        WFSpecTopology.addStuff(topology, config);

        WFEventProcessorActor actor = new NullWFEventActor();
        WFRunTopology.addStuff(
            topology,
            config,
            config.getAllWFRunTopicsPattern(),
            actor
        );

        KafkaStreams streams = new KafkaStreams(topology, config.getStreamsConfig());

        APIStreamsContext context = new APIStreamsContext(streams);
        context.setWFSpecNameStoreName(Constants.WF_SPEC_NAME_STORE);
        context.setWFSpecGuidStoreName(Constants.WF_SPEC_GUID_STORE);
        context.setTaskDefGuidStoreName(Constants.TASK_DEF_GUID_STORE);
        context.setTaskDefNameStoreName(Constants.TASK_DEF_NAME_STORE);
        context.setWFRunStoreName(Constants.WF_RUN_STORE);

        LittleHorseAPI lapi = new LittleHorseAPI(config, context);

        Properties props = config.getConsumerConfig("wfSpecDeployer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
            props
        );
        consumer.subscribe(Collections.singletonList(config.getWFSpecActionsTopic()));
        WFSpecDeployer deployer = new WFSpecDeployer(consumer, config);
        Thread deployerThread = new Thread(() -> deployer.run());

        Runtime.getRuntime().addShutdownHook(new Thread(deployer::shutdown));
        Runtime.getRuntime().addShutdownHook(new Thread(config::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(lapi::cleanup));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        deployerThread.start();
        streams.start();
        lapi.run();
    }
}


class DaemonApp {
    public static void run() throws Exception {
        Config config = new Config();

        // just need to set up the topology and run it.

        WFSpec wfSpec = WFSpec.fromIdentifier(config.getWfSpecGuid(), config);

        NodeSchema node = wfSpec.getModel().nodes.get(config.getNodeName());
        TaskDef td = TaskDef.fromIdentifier(node.taskDefinitionName, config);

        WFEventProcessorActor actor = new TaskDaemonEventActor(
            wfSpec,
            node,
            td,
            config
        );

        Pattern pattern = Pattern.compile(wfSpec.getModel().kafkaTopic);
        Topology topology = new Topology();

        WFRunTopology.addStuff(topology, config, pattern, actor);
        KafkaStreams streams = new KafkaStreams(
            topology,
            config.getStreamsConfig(config.getNodeName())
        );
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}

class Thing {
    public String mystring;
    public Object foobar;
}

public class App {
    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("daemon")) {
            try {
                DaemonApp.run();
            } catch(Exception exn) {
                exn.printStackTrace();
            }
        } else if (args.length > 0 && args[0].equals("api")) {
            FrontendAPIApp.run();
        } else {
            String json = "{\"name\": \"task1\", \"guid\": \"06ab9216-a34c-4845-b594-4b1a90e8d3ee\", \"dockerImage\": \"little-horse-daemon\", \"bashCommand\": [\"python3\", \"/examples/task1.py\", \"<<personName>>\"], \"stdin\": null}";

            LHDeserializer<TaskDefSchema> deser = new LHDeserializer<>(
                TaskDefSchema.class
            );
            System.out.println(deser.deserialize("foo", json.getBytes()));
            deser.close();
        }
    }
}
