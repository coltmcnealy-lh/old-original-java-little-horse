package little.horse.lib;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Config {
    private KafkaProducer<String, String> producer;
    private String appId;
    private String bootstrapServers;
    private Properties kafkaConfig;
    private String wfSpecTopic;
    private String kafkaTopicPrefix;

    public Config() {
        Properties conf = new Properties();
        
        String kTopicPrefix = System.getenv(Constants.KAFKA_TOPIC_PREFIX_KEY);
        this.kafkaTopicPrefix = (kTopicPrefix == null) ? "" : kTopicPrefix;

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
        this.producer = new KafkaProducer<String, String>(this.kafkaConfig);

        this.wfSpecTopic = this.kafkaTopicPrefix + Constants.SYSTEM_PREFIX + "WFSpec";
    }

    public String getWFSpecTopic() {
        return this.wfSpecTopic;
    }

    public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
        return (Future<RecordMetadata>) this.producer.send(record);
    }

    public Future<RecordMetadata> send(
            ProducerRecord<String, String> record,
            Callback callback) {
        return this.producer.send(record, callback);
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
}
