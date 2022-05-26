package little.horse.common.objects.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Bytes;

import little.horse.api.metadata.IndexRecordKey;
import little.horse.common.DepInjContext;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;

public abstract class GETable extends BaseSchema {
    public String name;
    private Date created;
    public Date updated;

    public Long lastUpdatedOffset;

    @JsonIgnore
    public static String typename;

    @JsonIgnore
    public static<T extends GETable> String getLHTypeName(Class<T> cls) {
        return cls.getSimpleName();
    }

    @JsonIgnore
    public static <T extends GETable> String getAPIPath(Class<T> cls) {
        return "/" + cls.getSimpleName();
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

    public static<T extends GETable> String getAPIPath(String id, Class<T> cls) {
        return "/" + cls.getSimpleName() + "/" + id;
    }

    public static<T extends GETable> String getSearchPath(
        String key, String value, Class<T> cls) {
        return "/search/" + cls.getSimpleName() + "/" + key + "/" + value;
    }

    public static<T extends GETable> String getListPath(Class<T> cls) {
        return "/list/" + cls.getSimpleName();
    }

    public static<T extends GETable> String getInternalWaitAPIPath(
        String id, String offset, String partition, Class<T> cls
    ) {
        return String.format(
            "/internal/waitFor/%s/%s/%s/%s",
            cls.getSimpleName(), id, offset, partition
        );
    }

    public static <T extends GETable> String getInternalIterLabelsAPIPath(
        String start, String end, String token, Class<T> cls
    ) {
        return String.format(
            "/internal/%s/iterLabels/%s/%s/%s",
            cls.getSimpleName(), start, end, token
        );
    }

    public static <T extends GETable> String getInternalIterLabelsAPIPath(
        String start, String end, String token, String limit, Class<T> cls
    ) {
        return String.format(
            "/internal/%s/iterLabels/%s/%s/%s",
            cls.getSimpleName(), start, end, token
        ) + "?limit=" + limit;
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

    // @JsonIgnore
    public Map<String, String> getIndexKeyValPairs() {
        HashMap<String, String> out = new HashMap<>();
        out.put("name", name);
        if (updated == null) updated = getCreated();
        out.put("created", LHUtil.dateToDbString(getCreated()));
        out.put("updated", LHUtil.dateToDbString(updated));
        return out;
    }

    @JsonIgnore
    public Set<IndexRecordKey> getIndexEntries() {
        HashSet<IndexRecordKey> out = new HashSet<>();
        for (Map.Entry<String, String> e : getIndexKeyValPairs().entrySet()) {
            out.add(new IndexRecordKey(e.getKey(), e.getValue(), this));
        }
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
