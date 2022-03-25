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
import org.apache.kafka.common.utils.Bytes;

import io.javalin.Javalin;
import little.horse.api.metadata.AliasIdentifier;
import little.horse.common.DepInjContext;
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
    public static void overridePostAPIEndpoints(Javalin app, DepInjContext config) {}

    @DigestIgnore
    public Long lastUpdatedOffset;

    @JsonIgnore
    public static String typename;

    @JsonIgnore
    public static<T extends CoreMetadata> String getLHTypeName(Class<T> cls) {
        return cls.getSimpleName();
    }

    @JsonIgnore
    public static <T extends CoreMetadata> String getIdKafkaTopic(
        DepInjContext config, Class<T> cls
    ) {
        return config.getKafkaTopicPrefix() + "__" + T.getLHTypeName(cls);
    }

    @JsonIgnore
    public static<T extends CoreMetadata> String getAliasKafkaTopic(
        DepInjContext config, Class<T> cls
    ) {
        return getIdKafkaTopic(config, cls) + "__aliases";
    }

    @JsonIgnore
    public static<T extends CoreMetadata> String getIdStoreName(Class<T> cls) {
        return T.getLHTypeName(cls);
    }

    @JsonIgnore
    public static<T extends CoreMetadata> String getAliasStoreName(Class<T> cls) {
        return getIdStoreName(cls) + "__aliases";
    }

    public static<T extends CoreMetadata> String getAPIPath(Class<T> cls) {
        return "/" + T.getLHTypeName(cls);
    }

    public static<T extends CoreMetadata> String getAPIPath(String id, Class<T> cls) {
        return getAPIPath(cls) + "/" + id;
    }

    public static<T extends CoreMetadata> String getAliasSetPath(Class<T> cls) {
        return getAPIPath(cls) + "AliasSet";
    }

    public static<T extends CoreMetadata> String getAliasPath(Class<T> cls) {
        return getAPIPath(cls) + "Alias";
    }

    public static<T extends CoreMetadata> String getAllAPIPath(Class<T> cls) {
        return getAPIPath(cls) + "All";
    }

    public static<T extends CoreMetadata> String getAliasPath(
        String aliasName, String aliasValue, Class<T> cls) {
        return getAliasPath(cls) + "/" + aliasName + "/" + aliasValue;
    }

    public static<T extends CoreMetadata> String getAliasSetPath(
        String aliasName, String aliasValue, Class<T> cls) {
        return getAliasSetPath(cls) + "/" + aliasName + "/" + aliasValue;
    }

    public static<T extends CoreMetadata> String getAliasPath(
        Map<String, String> aliases, Class<T> cls
    ) {
        String path = getAliasPath(cls);
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

    public static<T extends CoreMetadata> String getWaitForAPIPath(
        String id, String offset, String partition, Class<T> cls
    ) {
        return getAPIPath(cls) + "Offset/" + id + "/" + offset + "/" + partition;
    }

    public static<T extends CoreMetadata> String getWaitForAPIPath(
        String id, long offset, int partition, Class<T> cls
    ) {
        return getAPIPath(cls) + "Offset/" + id + "/" + String.valueOf(offset)
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
        ProducerRecord<String, Bytes> record = new ProducerRecord<String, Bytes>(
            getIdKafkaTopic(this.config, this.getClass()),
            getId(),
            new Bytes(this.toBytes())
        );
        return this.config.send(record);
    }

    @JsonIgnore
    public static<T extends CoreMetadata> Future<RecordMetadata> sendNullRecord(
        String id, DepInjContext config, Class<T> cls
    ) {
        ProducerRecord<String, Bytes> record = new ProducerRecord<String, Bytes>(
            getIdKafkaTopic(config, cls), id, null);
        return config.send(record);
    }

    public abstract void validate(DepInjContext config)
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
