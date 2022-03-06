package little.horse.common;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;

import little.horse.common.util.Constants;
import little.horse.lib.deployers.NullTaskDeployer;
import little.horse.lib.deployers.TaskDeployer;
import little.horse.lib.deployers.WorkflowDeployer;
import little.horse.lib.deployers.docker.DockerTaskDeployer;
import little.horse.lib.deployers.docker.DockerWFSpecDeployer;
import okhttp3.OkHttpClient;


public class Config {
    private KafkaProducer<String, Bytes> txnProducer;
    private KafkaProducer<String, Bytes> producer;
    
    private HashMap<String, KafkaConsumer<String, Bytes>> consumers;

    private String appId;
    private String appInstanceId;
    public String bootstrapServers;
    private String kafkaTopicPrefix;
    private String stateDirectory;
    private String apiURL;
    private OkHttpClient httpClient;
    private int defaultReplicas;
    private Admin kafkaAdmin;
    private int defaultPartitions;
    private String wfWorkerImage;
    // private String wfSpecGuid;
    // private String wfNodeName;
    // private String threadSpecName;
    private String advertisedHost;
    private int advertisedPort;
    private String advertisedProtocol;

    public String getAppId() {
        return this.appId;
    }

    public Config() {
        // TODO: Make this more readable

        // ********* Kafka Stuff *************

        String theAppId = System.getenv(Constants.KAFKA_APPLICATION_ID_KEY);
        this.appId = this.kafkaTopicPrefix + ((theAppId == null) ? "test" : appId);
        String theIid = System.getenv(Constants.KAFKA_APPLICATION_IID_KEY);
        this.appInstanceId = this.kafkaTopicPrefix + ((theIid == null) ?
            "first" : appId);

        String booty = System.getenv(Constants.KAFKA_BOOTSTRAP_SERVERS_KEY);
        this.bootstrapServers = (booty == null) ? "host.docker.internal:9092" : booty;

        // ************* Misc env var stuff **********

        String kTopicPrefix = System.getenv(Constants.KAFKA_TOPIC_PREFIX_KEY);
        this.kafkaTopicPrefix = (kTopicPrefix == null) ? "" : kTopicPrefix;

        String theHost = System.getenv(Constants.ADVERTISED_HOST_KEY);
        this.advertisedHost = (theHost == null) ? "localhost" : theHost;

        String theProto = System.getenv(Constants.ADVERTISED_PROTOCOL_KEY);
        this.advertisedProtocol= (theProto == null) ? "http" : theProto;

        String thePort = System.getenv(Constants.ADVERTISED_PORT_KEY);
        this.advertisedPort = thePort == null ? 80 : Integer.valueOf(thePort);

        String sdir = System.getenv(Constants.STATE_DIR_KEY);
        this.stateDirectory = (sdir == null) ? "/tmp/kafkaState" : sdir;

        String tempApiURL = System.getenv(Constants.API_URL_KEY);
        this.apiURL = (tempApiURL == null) ? "http://host.docker.internal:30000" : tempApiURL;

        String tempReplicas = System.getenv(Constants.DEFAULT_REPLICAS_KEY);
        try {
            defaultReplicas = Integer.valueOf(tempReplicas);
        } catch (Exception exn) {
            System.err.println(exn.getMessage());
            defaultReplicas = 1;
        }

        this.httpClient = new OkHttpClient();

        Properties akProperties = new Properties();
        akProperties.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            this.bootstrapServers
        );
        this.kafkaAdmin = Admin.create(akProperties);

        String tempParts = System.getenv(Constants.DEFAULT_PARTITIONS_KEY);
        try {
            defaultPartitions = Integer.valueOf(tempParts);
        } catch (Exception exn) {
            defaultPartitions = 1;
        }

        String tempCollectorImage = System.getenv(Constants.DEFAULT_WF_WORKER_IMAGE_KEY);
        this.wfWorkerImage = (
            tempCollectorImage == null
        ) ? "little-horse-api:latest" : tempCollectorImage;

        // this.wfSpecGuid = System.getenv(Constants.WF_SPEC_ID_KEY);
        // this.wfNodeName = System.getenv(Constants.NODE_NAME_KEY);
        // this.threadSpecName = System.getenv(Constants.THREAD_SPEC_NAME_KEY);
    }

    public HostInfo getHostInfo() {
        return new HostInfo(advertisedHost, advertisedPort);
    }

    public void createKafkaTopic(NewTopic topic) {
        CreateTopicsResult result = kafkaAdmin.createTopics(
            Collections.singleton(topic)
        );
        KafkaFuture<Void> future = result.values().get(topic.name());
        try {
            future.get();
        } catch (Exception exn) {
            exn.printStackTrace();
            // throw new RuntimeException("Failed to create kafka topic");
        }
    }

    public String getWfWorkerImage() {
        return this.wfWorkerImage;
    }

    public ArrayList<String> getCollectorCommand() {
        ArrayList<String> out = new ArrayList<String>();
        out.add("java");
        out.add("-jar");
        out.add("/littleHorse.jar");
        out.add("collector");
        return out;
    }

    public OkHttpClient getHttpClient() {
        return this.httpClient;
    }

    public String getAPIUrlFor(String extension) {
        String out = this.getAPIUrl();
        if (!extension.startsWith("/")) {
            out += "/";
        }
        out += extension;
        return out;
    }

    public String getAPIUrl() {
        return this.apiURL;
    }

    public ArrayList<String> getTaskDaemonCommand() {
        ArrayList<String> out = new ArrayList<String>();
        out.add("java");
        out.add("-jar");
        out.add("/littleHorse.jar");
        out.add("workflow-worker");
        return out;
    }

    public String getTaskDeployerClassName() {
        return NullTaskDeployer.class.getCanonicalName();
    }

    public TaskDeployer getTaskDeployer() {
        String clsnm = getTaskDeployerClassName();
        Class<?> cls;

        try {
            cls = Class.forName(clsnm);
        } catch (ClassNotFoundException exn) {
            throw new RuntimeException(
                "Unable to find provided classname " + clsnm + ": "
                + exn.getMessage(), exn
            );
        }

        try {
            return TaskDeployer.class.cast(
                cls.getDeclaredConstructor().newInstance()
            );
        } catch(IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException 
                | InstantiationException exn) {
            throw new RuntimeException(
                "Unable to instantiate Object of type " + clsnm + ": " +
                exn.getMessage(), exn
            );
        }
    }
    
    public String getWorkflowDeployerClassName() {
        return DockerWFSpecDeployer.class.getCanonicalName();
    }

    public WorkflowDeployer getWorkflowDeployer() {
        String clsnm = getWorkflowDeployerClassName();
        Class<?> cls;

        try {
            cls = Class.forName(clsnm);
        } catch (ClassNotFoundException exn) {
            throw new RuntimeException(
                "Unable to find provided classname " + clsnm + ": "
                + exn.getMessage(), exn
            );
        }

        try {
            return WorkflowDeployer.class.cast(
                cls.getDeclaredConstructor().newInstance()
            );
        } catch(IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException 
                | InstantiationException exn) {
            throw new RuntimeException(
                "Unable to instantiate Object of type " + clsnm + ": " +
                exn.getMessage(), exn
            );
        }
    }

    public HashMap<String, String> getBaseEnv() {
        HashMap<String, String> out = new HashMap<String, String>();
        out.put(Constants.API_URL_KEY, this.getAPIUrl());
        out.put(Constants.STATE_DIR_KEY, this.stateDirectory);
        out.put(
            Constants.KAFKA_BOOTSTRAP_SERVERS_KEY, this.bootstrapServers
        );
        out.put(
            Constants.KAFKA_TOPIC_PREFIX_KEY, this.kafkaTopicPrefix
        );
        return out;
    }

    public String getKafkaTopicPrefix() {
        return kafkaTopicPrefix;
    }

    public int getDefaultReplicas() {
        return this.defaultReplicas;
    }

    public KafkaProducer<String, Bytes> getTxnProducer() {
        if (this.txnProducer == null) {
            Properties conf = new Properties();
            conf.put("bootstrap.servers", this.bootstrapServers);
            conf.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
            );
            conf.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer"
            );
            conf.put("transactional.id", this.appId);
            conf.put("enable.idempotence", "true");

            this.txnProducer = new KafkaProducer<String, Bytes>(conf);   
            this.txnProducer.initTransactions(); 
        }

        return this.txnProducer;
    }

    public KafkaProducer<String, Bytes> getProducer() {
        if (this.producer == null) {
            Properties conf = new Properties();
            conf.put("bootstrap.servers", this.bootstrapServers);
            conf.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
            );
            conf.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer"
            );
            conf.put("enable.idempotence", "true");

            this.producer = new KafkaProducer<String, Bytes>(conf);
        }

        return this.producer;
    }

    /**
     * Returns a kafka consumer configured to consume from the kafka broker. The
     * result is NOT threadsafe and is a singleton for the entire application. The
     * result has properly-configured group.id and group.instance.id. The result has
     * disabled automatic offset committing as it is intended that all consumption in
     * LittleHorse will be done in an exactly-once manner; meaning that automatic
     * offset commits are unpalatable.
     * @return a KafkaConsumer<String, Bytes>
     */
    public KafkaConsumer<String, Bytes> getConsumer(String topic) {
        if (consumers == null) consumers = new HashMap<>();

        if (consumers.containsKey(topic)) {
            throw new RuntimeException(
                "Tried to get same consumer twice for topic " + topic + " which is" +
                " not threadsafe!"
            );
        }

        Properties conf = new Properties();
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, this.appId);
        conf.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.appInstanceId);
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        conf.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        conf.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class
        );
        conf.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.BytesDeserializer.class
        );

        KafkaConsumer<String, Bytes> cons = new KafkaConsumer<String, Bytes>(conf);
        cons.subscribe(Collections.singleton(topic));
        consumers.put(topic, cons);
        return cons;
    }

    public Future<RecordMetadata> send(ProducerRecord<String, Bytes> record) {
        return (Future<RecordMetadata>) this.getProducer().send(record);
    }

    public Future<RecordMetadata> send(
            ProducerRecord<String, Bytes> record,
            Callback callback) {
        return this.getProducer().send(record, callback);
    }

    public String getAdvertisedUrl() {
        return String.format(
            "%s://%s:%d",
            advertisedProtocol,
            advertisedHost,
            advertisedPort
        );
    }

    public Duration getTaskPollDuration() {
        return Duration.ofMillis(10);
    }

    public Properties getStreamsConfig(String appIdSuffix) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.appId + appIdSuffix);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, this.getAdvertisedUrl());
        props.put(StreamsConfig.STATE_DIR_CONFIG, this.stateDirectory);
        props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, 4000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName()
        );
        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName()
        );
        props.put(
            StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), 4000
        );
        props.put(
            StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class
        );
        props.put(
            StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
            org.apache.kafka.streams.errors.DefaultProductionExceptionHandler.class
        );
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, "all");
        return props;
    }

    /**
     * Cleans up this object (i.e. closes any kafka producers or consumers, etc).
     * Should only be called at the end of lifecycle; suggested for use in a try/finally
     * block. Due to lack of Deconstructor in java.
     */
    public void cleanup() {
        if (this.txnProducer != null) this.txnProducer.close();
        if (this.producer != null) this.producer.close();
        if (this.kafkaAdmin != null) this.kafkaAdmin.close();

        if (this.consumers != null) {
            for (KafkaConsumer<String, Bytes> cons: consumers.values()) {
                cons.close();
            }
        }
    }

    public int getDefaultPartitions() {
        return this.defaultPartitions;
    }

    // /**
    //  * Gets the WFSpecGuid from Environment
    //  * @return the wfSpecGuid for this 
    //  */
    // public String getWfSpecId() {
    //     return this.wfSpecGuid;
    // }

    // public String getNodeName() {
    //     return this.wfNodeName;
    // }

    // public String getThreadSpecName() {
    //     return this.threadSpecName;
    // }

    public String getWFRunTopic() {
        return this.getKafkaTopicPrefix() + "wfRunEventLog";
    }

    public String getWFRunTopicPrefix() {
        return this.getKafkaTopicPrefix() + "wfEvents__";
    }

    public String getWFRunTopic(String wfRunGuid) {
        return this.getWFRunTopicPrefix() + "-" + wfRunGuid;
    }

    public Pattern getAllWFRunTopicsPattern() {
        return Pattern.compile(
            this.getWFRunTopicPrefix() + ".*"
        );
        // return Pattern.compile("my-wf_59a65ff8-d7f3-49d2-b744-e5a2e829bb82");
    }
}
