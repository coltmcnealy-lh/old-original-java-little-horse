package little.horse.common.objects.metadata;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.javalin.Javalin;
import little.horse.api.metadata.AliasIdentifier;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.DigestIgnore;

public abstract class CoreMetadata extends BaseSchema {
    public String name;
    public LHDeployStatus desiredStatus;
    public LHDeployStatus status;

    @JsonIgnore
    public static boolean onlyUseDefaultAPIforGET = false;
    public static void overridePostAPIEndpoints(Javalin app, Config config) {}

    @DigestIgnore
    public Long lastUpdatedOffset;

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

    public static String getAPIPath(String id) {
        return getAPIPath() + "/" + id;
    }

    public static String getAliasPath() {
        return getAPIPath() + "Alias";
    }

    public static String getAliasPath(String aliasName, String aliasValue) {
        return getAliasPath() + "/" + aliasName + "/" + aliasValue;
    }

    public static String getAliasPath(Map<String, String> aliases) {
        String path = getAliasPath();
        if (aliases.size() == 0) return path;

        path += "?";

        for (Map.Entry<String, String> param: aliases.entrySet()) {
            try {
                path += URLEncoder.encode(param.getKey(), "x-www-form-urlencoded");
            } catch(UnsupportedEncodingException exn) {
                exn.printStackTrace();
            }
            path += "&";
        }

        return path.substring(0, path.length() - 1);
    }

    public static String getWaitForAPIPath(
        String id, String offset, String partition
    ) {
        return getAPIPath() + "Offset/" + id + "/" + offset + "/" + partition;
    }

    public static String getWaitForAPIPath(
        String id, long offset, int partition
    ) {
        return getAPIPath() + "Offset/" + id + "/" + String.valueOf(offset)
            + "/" + String.valueOf(partition);
    }

    public abstract void processChange(CoreMetadata old) throws LHConnectionError;
    
    /**
     * Idempotent cleanup of resources when the CoreMetadata is deleted from the API.
     * For example, undeploys the WFRuntime deployer on WFSpec.
     */
    public void remove() throws LHConnectionError {
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

    public abstract void validate(Config config)
    throws LHValidationError, LHConnectionError;

    public Set<AliasIdentifier> getAliases() {
        HashSet<AliasIdentifier> out = new HashSet<>();
        AliasIdentifier i = new AliasIdentifier();
        i.aliasName = "name";
        i.aliasValue = name;
        out.add(i);
        return out;
    }
}
