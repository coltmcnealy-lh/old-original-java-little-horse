package little.horse.common.objects.metadata;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Bytes;

import little.horse.api.metadata.IndexKeyRecord;
import little.horse.common.DepInjContext;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;

public abstract class GETable extends BaseSchema {
    public String name;
    private Date created;

    public Long lastUpdatedOffset;

    @JsonIgnore
    public static String typename;

    @JsonIgnore
    public static<T extends GETable> String getLHTypeName(Class<T> cls) {
        return cls.getSimpleName();
    }

    @JsonIgnore
    public static <T extends GETable> String getIdKafkaTopic(
        DepInjContext config, Class<T> cls
    ) {
        return config.getKafkaTopicPrefix() + T.getLHTypeName(cls);
    }

    @JsonIgnore
    public static<T extends GETable> String getIndexKafkaTopic(
        DepInjContext config, Class<T> cls
    ) {
        return getIdKafkaTopic(config, cls) + "__index";
    }

    @JsonIgnore
    public static<T extends GETable> String getIdStoreName(Class<T> cls) {
        return T.getLHTypeName(cls);
    }

    @JsonIgnore
    public static<T extends GETable> String getIndexStoreName(Class<T> cls) {
        return getIdStoreName(cls) + "__index";
    }

    public static<T extends GETable> String getAPIPath(Class<T> cls) {
        return "/" + T.getLHTypeName(cls);
    }

    public static<T extends GETable> String getAPIPath(String id, Class<T> cls) {
        return getAPIPath(cls) + "/" + id;
    }

    public static<T extends GETable> String getAliasSetPath(Class<T> cls) {
        return getAPIPath(cls) + "AliasSet";
    }

    public static<T extends GETable> String getAliasPath(Class<T> cls) {
        return getAPIPath(cls) + "Alias";
    }

    public static<T extends GETable> String getAllAPIPath(Class<T> cls) {
        return getAPIPath(cls) + "All";
    }

    public static<T extends GETable> String getAliasPath(
        String aliasName, String aliasValue, Class<T> cls) {
        return getAliasPath(cls) + "/" + aliasName + "/" + aliasValue;
    }

    public static<T extends GETable> String getAliasSetPath(
        String aliasName, String aliasValue, Class<T> cls) {
        return getAliasSetPath(cls) + "/" + aliasName + "/" + aliasValue;
    }

    public static<T extends GETable> String getAliasPath(
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

    public static<T extends GETable> String getWaitForAPIPath(
        String id, String offset, String partition, Class<T> cls
    ) {
        return getAPIPath(cls) + "Offset/" + id + "/" + offset + "/" + partition;
    }

    public static<T extends GETable> String getWaitForAPIPath(
        String id, long offset, int partition, Class<T> cls
    ) {
        return getAPIPath(cls) + "Offset/" + id + "/" + String.valueOf(offset)
            + "/" + String.valueOf(partition);
    }

    @JsonIgnore
    public Future<RecordMetadata> save() {
        ProducerRecord<String, Bytes> record = new ProducerRecord<String, Bytes>(
            getIdKafkaTopic(this.config, this.getClass()),
            getObjectId(),
            new Bytes(this.toBytes())
        );
        return this.config.send(record);
    }

    @JsonIgnore
    public static<T extends GETable> Future<RecordMetadata> sendNullRecord(
        String id, DepInjContext config, Class<T> cls
    ) {
        ProducerRecord<String, Bytes> record = new ProducerRecord<String, Bytes>(
            getIdKafkaTopic(config, cls), id, null);
        return config.send(record);
    }

    public Set<IndexKeyRecord> getAliases() {
        HashSet<IndexKeyRecord> out = new HashSet<>();
        IndexKeyRecord i = new IndexKeyRecord();
        i.key = "name";
        i.value = name;
        out.add(i);
        return out;
    }

    public Date getCreated() {
        if (created == null) {
            created = LHUtil.now();
        }
        return created;
    }
    /**
     * ONLY TO BE CALLED BY JACKSON
     */
    public void setCreated(Date date) {
        this.created = date;
    }

    public String objectId;
    public String getObjectId() {
        if (objectId == null) {
            objectId = LHUtil.generateGuid();
        }
        return objectId;
    }

    /**
     * Just here for jackson stupidity. @humans: Don't call this.
     */
    public void setObjectId(String foo) {
        objectId = foo;
    }
}
