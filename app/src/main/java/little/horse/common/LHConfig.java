package little.horse.common;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.lang.RandomStringUtils;
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
import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.Transaction;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;
import little.horse.deployers.examples.docker.DockerTaskDeployer;
import little.horse.deployers.examples.docker.DockerWorkflowDeployer;
import okhttp3.OkHttpClient;


public class LHConfig {
    private Properties properties;

    private KafkaProducer<String, Bytes> txnProducer;
    private KafkaProducer<String, Bytes> producer;
    private HashMap<String, KafkaConsumer<String, Bytes>> consumers;

    private Admin kafkaAdmin;

    private OkHttpClient httpClient;


    public LHConfig() {
        this.initialize(getDefaultsFromEnv());
    }

    public LHConfig(Properties overrides) {
        Properties newProps = new Properties(getDefaultsFromEnv());

        for (String key: overrides.stringPropertyNames()) {
            newProps.setProperty(key, overrides.getProperty(key));
        }

        initialize(newProps);
    }

    // Kafka stuff

    /**
     * The application ID for this LittleHorse process (i.e. Container/Pod/etc).
     * For example, logical groups such as the Core API, the Workflow Workers for a
     * specific WFSpec, or the Task Workers for a specific TaskDef should all share
     * the same Application ID.
     * @return the Kafka Application ID (aka Consumer Group ID) for this LH Process.
     */
    public String getAppId() {
        return getOrSetDefault(
            Constants.KAFKA_APPLICATION_ID_KEY,
            "unset-app-id-badbadbad"
        );
    }
    public KafkaProducer<String, Bytes> getProducer() {
        if (this.producer == null) {
            Properties conf = new Properties();
            conf.put("bootstrap.servers", this.getBootstrapServers());
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
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, this.getAppId());
        conf.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.getAppInstanceId());
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
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

    public String getBootstrapServers() {
        return getOrSetDefault(
            Constants.KAFKA_BOOTSTRAP_SERVERS_KEY,
            "localhost:9092"
        );
    }

    public Future<RecordMetadata> send(ProducerRecord<String, Bytes> record) {
        return (Future<RecordMetadata>) this.getProducer().send(record);
    }

    public Future<RecordMetadata> send(
            ProducerRecord<String, Bytes> record,
            Callback callback) {
        return this.getProducer().send(record, callback);
    }

    public String getAppInstanceId() {
        String defVal = getKafkaTopicPrefix();
        defVal += RandomStringUtils.randomAlphanumeric(10).toLowerCase();
        defVal += "-unset-instance-id-badbadbad";

        return getOrSetDefault(
            Constants.KAFKA_APPLICATION_IID_KEY,
            defVal
        );
    }
    public String getKafkaTopicPrefix() {
        return String.class.cast(properties.getOrDefault(
            Constants.KAFKA_TOPIC_PREFIX_KEY,
            ""
        ));
    }

    public String getStateDirectory() {
        return getOrSetDefault(
            Constants.STATE_DIR_KEY,
            "/tmp/kafkaState"
        );
    }

    public KafkaProducer<String, Bytes> getTxnProducer() {
        if (this.txnProducer == null) {
            Properties conf = new Properties();
            conf.put("bootstrap.servers", this.getBootstrapServers());
            conf.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
            );
            conf.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer"
            );
            conf.put("transactional.id", getAppId() + "-_-" + getAppInstanceId());
            conf.put("enable.idempotence", "true");

            this.txnProducer = new KafkaProducer<String, Bytes>(conf);   
            this.txnProducer.initTransactions(); 
        }

        return this.txnProducer;
    }

    public Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            this.getAppId()
        );
        props.put(
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.getAppInstanceId()
        );
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, this.getAdvertisedUrl());
        props.put(StreamsConfig.STATE_DIR_CONFIG, this.getStateDirectory());
        props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, 4000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "at_least_once");
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

    // Worker Configuration
    public String getWfWorkerImage() {
        return getOrSetDefault(
            Constants.DEFAULT_WF_WORKER_IMAGE_KEY,
            "little-horse-api:latest"
        );
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
        return getOrSetDefault(
            Constants.API_URL_KEY,
            "http://localhost:5000"
        );
    }

    public int getDefaultReplicas() {
        return Integer.valueOf(String.class.cast(properties.getOrDefault(
            Constants.DEFAULT_REPLICAS_KEY, "1"
        )));
    }

    public String getDefaultTaskDeployerClassName() {
        return String.class.cast(properties.getOrDefault(
            Constants.DEFAULT_TASK_DEPLOYER_KEY,
            DockerTaskDeployer.class.getCanonicalName()
        ));
    }

    public String getDefaultWFDeployerClassName() {
        return String.class.cast(properties.getOrDefault(
            Constants.DEFAULT_WF_DEPLOYER_KEY,
            DockerWorkflowDeployer.class.getCanonicalName()
        ));
    }

    public HashMap<String, String> getBaseEnv() {
        HashMap<String, String> out = new HashMap<String, String>();
        out.put(Constants.API_URL_KEY, this.getAPIUrl());
        out.put(Constants.STATE_DIR_KEY, this.getStateDirectory());

        out.put(
            Constants.KAFKA_BOOTSTRAP_SERVERS_KEY, this.getBootstrapServers()
        );
        out.put(
            Constants.KAFKA_TOPIC_PREFIX_KEY, this.getKafkaTopicPrefix()
        );
        return out;
    }

    // Info about the Advertised Host for this process

    public HostInfo getHostInfo() {
        return new HostInfo(getAdvertisedHost(), getAdvertisedPort());
    }

    public String getAdvertisedProto() {
        return getOrSetDefault(
            Constants.ADVERTISED_PROTOCOL_KEY, "http"
        );
    }

    public String getAdvertisedUrl() {

        return String.format(
            "%s://%s:%d",
            getAdvertisedProto(),
            this.getAdvertisedHost(),
            getAdvertisedPort()
        );
    }

    public String getAdvertisedHost() {
        return getOrSetDefault(
            Constants.ADVERTISED_HOST_KEY, "localhost"
        );
    }

    public int getAdvertisedPort() {
        return Integer.valueOf(getOrSetDefault(
            Constants.ADVERTISED_PORT_KEY, "5000"
        ));
    }

    // HTTP Client
    public OkHttpClient getHttpClient() {
        return this.httpClient;
    }

    public Duration getTaskPollDuration() {
        return Duration.ofMillis(Integer.valueOf(String.class.cast(
            properties.getOrDefault(
                Constants.DEFAULT_TASK_WORKER_POLL_MILLIS_KEY, "10"
            )
        )));
    }

    public boolean getShouldExposeHealth() {
        return getOrSetDefault(
            Constants.EXPOSE_KSTREAMS_HEALTH_KEY, ""
        ).equals("true");
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

    // Thin wrapper used for depency injection
    public <T extends Object> T loadClass(String className) {
        return LHUtil.loadClass(className);
    }

    public int getDefaultPartitions() {
        return Integer.valueOf(getOrSetDefault(Constants.DEFAULT_PARTITIONS_KEY, "1"));
    }

    public String getWFRunEventTopicPrefix() {
        return this.getKafkaTopicPrefix() + "wfEvents__";
    }

    public String getWFRunTimerTopicPrefix() {
        return this.getKafkaTopicPrefix() + "timers__";
    }

    public Pattern getAllWFRunTopicsPattern() {
        return Pattern.compile(
            this.getWFRunEventTopicPrefix() + ".*"
        );
    }

    // Private Implementation Utility methods
    private String getOrSetDefault(String key, String defaultVal) {
        String result = String.class.cast(properties.get(key));

        if (result == null) {
            properties.setProperty(key, defaultVal);
            return defaultVal;
        } else {
            return result;
        }
    }

    public String getDbHost() {
        return getOrSetDefault(Constants.DB_HOST_KEY, "localhost");
    }

    public int getDbPort() {
        return Integer.valueOf(getOrSetDefault(Constants.DB_PORT_KEY, "5432"));
    }

    public String getDbUser() {
        return getOrSetDefault(Constants.DB_USER_KEY, "postgres");
    }

    public String getDbPassword() {
        return getOrSetDefault(Constants.DB_PASSWORD_KEY, "postgres");
    }

    public String getDbDb() {
        return getOrSetDefault(Constants.DB_DB_KEY, "postgres");
    }

    public Database getEbeanDb() {
        DataSourceConfig dSource = new DataSourceConfig();
        dSource.setUsername(getDbUser());
        dSource.setPassword(getDbPassword());

        dSource.setUrl(String.format("jdbc:postgresql://%s:%d/%s", getDbHost(),
                getDbPort(), getDbDb()));

        dSource.setIsolationLevel(Transaction.SERIALIZABLE);

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataSourceConfig(dSource);

        return DatabaseFactory.create(cfg);
    }


    private Properties getDefaultsFromEnv() {
        Properties props = new Properties();

        for (Map.Entry<String, String> entry: System.getenv().entrySet()) {
            if (entry.getKey().startsWith("LHORSE")) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }

        return props;
    }

    private void initialize(Properties props) {
        this.properties = props;

        this.httpClient = new OkHttpClient();

        Properties akProperties = new Properties();
        akProperties.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            this.getBootstrapServers()
        );
        this.kafkaAdmin = Admin.create(akProperties);
    }
}
