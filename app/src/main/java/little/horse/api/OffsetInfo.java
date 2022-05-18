package little.horse.api;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.streams.processor.api.RecordMetadata;

import little.horse.common.objects.BaseSchema;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;

public class OffsetInfo extends BaseSchema {
    public long offset;
    public Date processTime;
    public Date recordTime;

    @JsonIgnore
    public static String getKey(RecordMetadata meta) {
        return OffsetInfo.getKey(meta.topic(), meta.partition());
    }

    @JsonIgnore
    public static String getKey(String topic, int partition) {
        return String.format(
            "%s__%s-%d",
            Constants.LATEST_OFFSET_ROCKSDB_KEY,
            topic,
            partition
        );
    }

    public OffsetInfo() {}

    public OffsetInfo(RecordMetadata meta, Date recordTime) {
        this.offset = meta.offset();
        this.processTime = LHUtil.now();
        this.recordTime = recordTime;
    }
}
