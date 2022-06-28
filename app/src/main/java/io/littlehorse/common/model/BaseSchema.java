package io.littlehorse.common.model;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.util.LHUtil;

public class BaseSchema {
    @JacksonInject
    protected LHConfig config;

    public BaseSchema(LHConfig config) {
        this.config = config;
    }

    // Use sparingly.
    public BaseSchema(){}

    public static <T extends BaseSchema> T fromString(
        String src, Class<T> valueType, LHConfig config
    ) throws LHSerdeError {
        return BaseSchema.fromBytes(
            src.getBytes(StandardCharsets.UTF_8), valueType, config
        );
    }

    public static <T extends BaseSchema> T fromBytes(
        byte[] src, Class<T> valueType, LHConfig config
    ) throws LHSerdeError {
        T result;
        try {
            result = LHUtil.getObjectMapper(config).readValue(src, valueType);
        } catch(JsonProcessingException parent) {
            parent.printStackTrace();
            throw new LHSerdeError(
                "Failed to process json: " + parent.getMessage(), parent
            );
        } catch(IOException exn) {
            exn.printStackTrace();
            throw new LHSerdeError("Had an IO Exception, ooph", exn);
        }
        result.setConfig(config);
        return result;
    }

    public String toString() {
        try {
            return LHUtil.getObjectMapper(config).writeValueAsString(this);
        } catch(Exception exn) {
            exn.printStackTrace();
            return null;
        }
    }

    public void setConfig(LHConfig config) {
        this.config = config;
    }

    public byte[] toBytes() {
        return toString().getBytes();
    }
}
