package io.littlehorse.api;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.Constants;
import io.littlehorse.common.util.LHUtil;

import org.apache.kafka.streams.processor.api.RecordMetadata;

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
