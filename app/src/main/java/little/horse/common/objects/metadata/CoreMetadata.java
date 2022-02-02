package little.horse.common.objects.metadata;

import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.rundata.LHStatus;

public abstract class CoreMetadata extends BaseSchema {
    public String name;
    public String guid;
    public LHStatus desiredStatus;
    public LHStatus status;
    public static String typeName;

    public static String getKafkaTopic(Config config) {
        return config.getKafkaTopicPrefix() + "__" + typeName;
    }

    public static String getNameKeyedKafkaTopic(Config config) {
        return getKafkaTopic(config) + "__nameKeyed";
    }

    public static String getStoreName() {
        return typeName;
    }

    public static String getNameStoreName() {
        return typeName + "__nameKeyed";
    }

    public static String getOffsetStoreName() {
        return typeName + "__offset";
    }

    public abstract void processChange(CoreMetadata old);
}
