package little.horse.common.objects.metadata;

import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHDeployStatus;
import little.horse.common.util.LHUtil;

public abstract class CoreMetadata extends BaseSchema {
    public String name;
    public LHDeployStatus desiredStatus;
    public LHDeployStatus status;
    
    @JsonIgnore
    public static String typeName;

    @JsonIgnore
    public static String getEventKafkaTopic(Config config) {
        return config.getKafkaTopicPrefix() + "__" + typeName;
    }

    @JsonIgnore
    public static String getNameKeyedKafkaTopic(Config config) {
        return getEventKafkaTopic(config) + "__nameKeyed";
    }

    @JsonIgnore
    public static String getStoreName() {
        return typeName;
    }

    @JsonIgnore
    public static String getNameStoreName() {
        return typeName + "__nameKeyed";
    }

    @JsonIgnore
    public static String getOffsetStoreName() {
        return typeName + "__offset";
    }

    public static String getAPIPath() {
        return "/" + typeName;
    }

    public abstract void processChange(CoreMetadata old);
    
    /**
     * Idempotent cleanup of resources when the CoreMetadata is deleted from the API.
     * For example, undeploys the WFRuntime deployer on WFSpec.
     */
    public void remove() {
        // Nothing to do in default.
    }

    @JsonIgnore
    public Future<RecordMetadata> record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            getEventKafkaTopic(this.config), getDigest(), this.toString());
        return this.config.send(record);
    }

    public void undeploy() {
        LHUtil.log("TODO: write this function.");
    }
}
