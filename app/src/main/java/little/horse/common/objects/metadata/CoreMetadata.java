package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.producer.ProducerRecord;

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

    public abstract void processChange(CoreMetadata old);

    @JsonIgnore
    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            getEventKafkaTopic(this.config), getDigest(), this.toString());
        this.config.send(record);
    }  
    
    @JsonIgnore
    public CoreMetadata lookupOrCreate(
        String name,
        String guid,
        CoreMetadata spec,
        Class<? extends CoreMetadata> cls
    ) throws LHValidationError, LHConnectionError {
        throw new RuntimeException("Implement me!");

        /*
        if (spec != null) {
            if (guid != null && spec.getGuid() != guid) {
                throw new LHValidationError("Guid and spec don't match!");
            }
        }

        if (spec != null) return spec;

        CoreMetadata old = LHDatabaseClient.lookupMetadata(name, guid, cls);
        */
    }
}
