package io.littlehorse.common.model;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.util.LHUtil;

public class BaseSchema {
    public BaseSchema(){}

    public static <T extends BaseSchema> T fromString(
        String src, Class<T> valueType
    ) throws LHSerdeError {
        return BaseSchema.fromBytes(
            src.getBytes(StandardCharsets.UTF_8), valueType
        );
    }

    public static <T extends BaseSchema> T fromBytes(
        byte[] src, Class<T> valueType
    ) throws LHSerdeError {
        T result;
        try {
            result = LHUtil.getObjectMapper().readValue(src, valueType);
        } catch(JsonProcessingException parent) {
            parent.printStackTrace();
            throw new LHSerdeError(
                "Failed to process json: " + parent.getMessage(), parent
            );
        } catch(IOException exn) {
            exn.printStackTrace();
            throw new LHSerdeError("Had an IO Exception, ooph", exn);
        }
        return result;
    }

    public String toString() {
        try {
            return LHUtil.getObjectMapper().writeValueAsString(this);
        } catch(Exception exn) {
            exn.printStackTrace();
            return null;
        }
    }

    public byte[] toBytes() {
        return toString().getBytes();
    }
}
