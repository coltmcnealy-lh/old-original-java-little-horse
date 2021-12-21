package little.horse.lib;

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.kafka.streams.StreamsConfig;

import little.horse.lib.K8sStuff.EnvEntry;
import little.horse.lib.schemas.WFRunSchema;
import okhttp3.OkHttpClient;


public class Config {
    private KafkaProducer<String, String> producer;
    private String appId;
    public String bootstrapServers;
    private Properties kafkaConfig;
    private String wfSpecTopic;
    private String taskDeftopic;
    private String externalEventDefTopic;
    private String kafkaTopicPrefix;
    private String advertisedUrl;
    private String stateDirectory;
    private String defaultTaskDockerImage;
    private String apiURL;
    private OkHttpClient httpClient;
    private int defaultReplicas;
    private Admin kafkaAdmin;
    private int defaultPartitions;
    private String collectorImage;
    private String wfSpecGuid;
    private String wfNodeName;

    public Config() {
        // TODO: Make this more readable

        // ********* Kafka Config *************
        Properties conf = new Properties();

        String theAppId = System.getenv(Constants.KAFKA_APPLICATION_ID_KEY);
        this.appId = this.kafkaTopicPrefix + ((theAppId == null) ? "test" : appId);
        conf.put("client.id", this.appId);

        String booty = System.getenv(Constants.KAFKA_BOOTSTRAP_SERVERS_KEY);
        this.bootstrapServers = (booty == null) ? "host.docker.internal:39092" : booty;
        conf.put("bootstrap.servers", this.bootstrapServers);

        conf.put(
            "key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer"
        );
        conf.put(
            "value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer"
        );
        this.kafkaConfig = conf;


        // ************* Producer ************
        this.producer = new KafkaProducer<String, String>(this.kafkaConfig);

        // ************* Misc env var stuff **********

        String kTopicPrefix = System.getenv(Constants.KAFKA_TOPIC_PREFIX_KEY);
        this.kafkaTopicPrefix = (kTopicPrefix == null) ? "" : kTopicPrefix;

        this.wfSpecTopic = this.kafkaTopicPrefix + Constants.SYSTEM_PREFIX + "WFSpec";
        this.taskDeftopic = this.kafkaTopicPrefix + Constants.SYSTEM_PREFIX + "TaskDef";
        this.externalEventDefTopic = this.kafkaTopicPrefix + Constants.SYSTEM_PREFIX + "ExternalEventDef";

        String theURL = System.getenv(Constants.ADVERTISED_URL_KEY);
        this.advertisedUrl = (theURL == null) ? "http://localhost:5000" : theURL;

        String sdir = System.getenv(Constants.STATE_DIR_KEY);
        this.stateDirectory = (sdir == null) ? "/tmp/kafkaState" : sdir;

        String drmg = System.getenv(Constants.DEFAULT_TASK_IMAGE_KEY);
        this.defaultTaskDockerImage = (drmg == null) ? "little-horse-daemon:latest": drmg;

        String tempApiURL = System.getenv(Constants.API_URL_KEY);
        this.apiURL = (tempApiURL == null) ? "http://localhost:5000" : tempApiURL;

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

        String tempCollectorImage = System.getenv(Constants.DEFAULT_COLLECTOR_IMAGE_KEY);
        this.collectorImage = (
            tempCollectorImage == null
        ) ? "little-horse-collector:latest" : tempCollectorImage;

        this.wfSpecGuid = System.getenv(Constants.WF_SPEC_GUID_KEY);
        this.wfNodeName = System.getenv(Constants.NODE_NAME_KEY);
    }

    public void createKafkaTopic(NewTopic topic) {
        CreateTopicsResult result = kafkaAdmin.createTopics(
            Collections.singleton(topic)
        );
        KafkaFuture<Void> future = result.values().get(topic.name());
        try {
            future.get();
        } catch (Exception any) {
            // TODO: handle the orzdash
        }
    }

    public String getCollectorImage() {
        return this.collectorImage;
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

    public String getWFSpecTopic() {
        return this.wfSpecTopic;
    }

    public String getAPIUrlFor(String extension) {
        return this.getAPIUrl() + "/" + extension;
    }

    public String getAPIUrl() {
        return this.apiURL;
    }

    public String getTaskDefTopic() {
        return this.taskDeftopic;
    }

    public String getExternalEventDefTopic() {
        return this.externalEventDefTopic;
    }

    public String getTaskDefNameKeyedTopic() {
        return this.taskDeftopic + "__nameKeyed";
    }

    public String getExternalEventDefNameKeyedTopic() {
        return this.externalEventDefTopic + "__nameKeyed";
    }

    public String getWFSpecActionsTopic() {
        return this.wfSpecTopic + "__actions";
    }

    public String getWFSpecNameKeyedTopic() {
        return this.wfSpecTopic + "__nameKeyed";
    }

    public String getWFSpecIntermediateTopic() {
        return this.wfSpecTopic + "__intermediate";
    }

    public ArrayList<String> getTaskDaemonCommand() {
        ArrayList<String> out = new ArrayList<String>();
        out.add("java");
        out.add("-jar");
        out.add("/littleHorse.jar");
        out.add("daemon");
        return out;
    }

    public ArrayList<EnvEntry> getBaseK8sEnv() {
        ArrayList<EnvEntry> out = new ArrayList<EnvEntry>();
        out.add(new EnvEntry(Constants.API_URL_KEY, this.getAPIUrl()));
        out.add(new EnvEntry(Constants.STATE_DIR_KEY, this.stateDirectory));
        out.add(new EnvEntry(
            Constants.KAFKA_BOOTSTRAP_SERVERS_KEY, this.bootstrapServers
        ));
        out.add(new EnvEntry(
            Constants.KAFKA_TOPIC_PREFIX_KEY, this.kafkaTopicPrefix
        ));
        return out;
    }

    public String getKafkaTopicPrefix() {
        return kafkaTopicPrefix;
    }

    public int getDefaultReplicas() {
        return this.defaultReplicas;
    }

    public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
        return (Future<RecordMetadata>) this.producer.send(record);
    }

    public Future<RecordMetadata> send(
            ProducerRecord<String, String> record,
            Callback callback) {
        return this.producer.send(record, callback);
    }

    public Properties getStreamsConfig() {
        return this.getStreamsConfig("");
    }

    public Properties getStreamsConfig(String appIdSuffix) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.appId + appIdSuffix);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, this.advertisedUrl);
        props.put(StreamsConfig.STATE_DIR_CONFIG, this.stateDirectory);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, (new Serdes.StringSerde()).getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, (new Serdes.StringSerde()).getClass().getName());
        props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, 15000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), 15000);
        return props;
    }

    public Properties getConsumerConfig(String appIdSuffix) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.appId + appIdSuffix);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        return props;
    }

    /**
     * Cleans up this object (i.e. closes any kafka producers or consumers, etc).
     * Should only be called at the end of lifecycle; suggested for use in a try/finally
     * block. Due to lack of Deconstructor in java.
     */
    public void cleanup() {
        this.producer.close();
        this.kafkaAdmin.close();
    }

    public String getDefaultTaskDockerImage() {
        return this.defaultTaskDockerImage;
    }

    public int getDefaultPartitions() {
        return this.defaultPartitions;
    }

    /**
     * Gets the WFSpecGuid from Environment
     * @return the wfSpecGuid for this 
     */
    public String getWfSpecGuid() {
        return this.wfSpecGuid;
    }

    public String getNodeName() {
        return this.wfNodeName;
    }

    public KafkaConsumer<String, WFRunSchema> getWFRunConsumer(
        ArrayList<String> topics, String appIdSuffix
    ) {
        Properties consumerConfig = getConsumerConfig(appIdSuffix);
        KafkaConsumer<String, WFRunSchema> cons = new KafkaConsumer<String, WFRunSchema>(
            consumerConfig
        );
        Runtime.getRuntime().addShutdownHook(new Thread(cons::close));
        cons.subscribe(topics);
        return cons;
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
