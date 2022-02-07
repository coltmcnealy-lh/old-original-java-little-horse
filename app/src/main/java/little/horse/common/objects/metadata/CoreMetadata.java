package little.horse.common.objects.metadata;

import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import little.horse.api.util.APIStreamsContext;
import little.horse.api.util.LHAPIPostResult;
import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHStatus;

public abstract class CoreMetadata extends BaseSchema {
    public String name;
    public LHStatus desiredStatus;
    public LHStatus status;
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

    @JsonIgnore
    public Future<RecordMetadata> record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            getEventKafkaTopic(this.config), getDigest(), this.toString());
        return this.config.send(record);
    }

    /**
     * Creates a CoreMetadata objct from this thing's spec if it doesn't exist. If it
     * __does__ already exist, then:
     * - if this spec clashes with existings, throws an LHValidationError.
     * - if this spec doesn't clash, updates if needed and returns nothing.
     * 
     * Should only be called by the LittleHorseAPI, and can only be called by that
     * as it requires the APIStreamsContext parameter.
     * @param context APIStreamsContext provided by the LittleHorseAPI object.
     * @return An LHAPIPostResult containing information about the thing that was
     * created.
     * @throws LHValidationError when this spec is invalid or if there is a conflict.
     * @throws LHConnectionError if we can't connect to some needed system.
     */
    @JsonIgnore
    public abstract<T extends CoreMetadata> LHAPIPostResult<T> createIfNotExists(
        APIStreamsContext context
    ) throws LHValidationError, LHConnectionError;

    public abstract boolean isEqualTo(CoreMetadata other);
}
