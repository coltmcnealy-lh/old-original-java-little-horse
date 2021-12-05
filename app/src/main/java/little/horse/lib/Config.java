package little.horse.lib;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.yaml.snakeyaml.scanner.Constant;


public class Config {
    private KafkaProducer<String, String> producer;
    private String appId;
    public String bootstrapServers;
    private Properties kafkaConfig;
    private String wfSpecTopic;
    private String taskDeftopic;
    private String kafkaTopicPrefix;
    private String advertisedUrl;
    private String stateDirectory;
    private String defaultTaskDockerImage;

    public Config() {
        // TODO: Make this more readable

        // ********* Kafka Config *************
        Properties conf = new Properties();

        String theAppId = System.getenv(Constants.KAFKA_APPLICATION_ID_KEY);
        this.appId = this.kafkaTopicPrefix + ((theAppId == null) ? "test" : appId);
        conf.put("client.id", this.appId);

        String booty = System.getenv(Constants.KAFKA_BOOTSTRAP_SERVERS_KEY);
        this.bootstrapServers = (booty == null) ? "host.docker.internal:9092" : booty;
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

        String theURL = System.getenv(Constants.ADVERTISED_URL_KEY);
        this.advertisedUrl = (theURL == null) ? "http://localhost:5000" : theURL;

        String sdir = System.getenv(Constants.STATE_DIR_KEY);
        this.stateDirectory = (sdir == null) ? "/tmp/kafkaState" : sdir;

        String drmg = System.getenv(Constants.DEFAULT_TASK_IMAGE_KEY);
        this.defaultTaskDockerImage = (drmg == null) ? "little-horse-task:latest": drmg;
    }

    public String getWFSpecTopic() {
        return this.wfSpecTopic;
    }

    public String getTaskDefTopic() {
        return this.taskDeftopic;
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
        return props;
    }

    /**
     * Cleans up this object (i.e. closes any kafka producers or consumers, etc).
     * Should only be called at the end of lifecycle; suggested for use in a try/finally
     * block. Due to lack of Deconstructor in java.
     */
    public void cleanup() {
        System.out.println("CLOSING");
        this.producer.close();
    }

    public String getDefaultTaskDockerImage() {
        return this.defaultTaskDockerImage;
    }
}
