package little.horse.common.objects.metadata;

import java.util.Set;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import little.horse.api.metadata.AliasIdentifier;
import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;

public abstract class CoreMetadata extends BaseSchema {
    public String name;
    public LHDeployStatus desiredStatus;
    public LHDeployStatus status;

    @JsonIgnore
    public static String typeName;

    @JsonIgnore
    public static String getIdKafkaTopic(Config config) {
        return config.getKafkaTopicPrefix() + "__" + typeName;
    }

    @JsonIgnore
    public static String getAliasKafkaTopic(Config config) {
        return getIdKafkaTopic(config) + "__aliases";
    }

    @JsonIgnore
    public static String getIdStoreName() {
        return typeName;
    }

    @JsonIgnore
    public static String getAliasStoreName() {
        return getIdStoreName() + "__aliases";
    }

    public static String getAPIPath() {
        return "/" + typeName;
    }

    public static String getAliasPath() {
        return getAPIPath() + "Alias";
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
    public Future<RecordMetadata> save() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            getIdKafkaTopic(this.config), getId(), this.toString());
        return this.config.send(record);
    }

    @JsonIgnore
    public static Future<RecordMetadata> sendNullRecord(String id, Config config) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            getIdKafkaTopic(config), id, null);
        return config.send(record);
    }

    public void undeploy() {
        LHUtil.log("TODO: write this function.");
    }

    public Set<AliasIdentifier> getAliases() {
        throw new RuntimeException("implement me!");
    }
}
