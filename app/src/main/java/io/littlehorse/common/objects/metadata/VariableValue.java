package io.littlehorse.common.objects.metadata;

import java.io.IOException;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.ebean.annotation.DbMigration.List;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.LHUtil;

public class VariableValue extends BaseSchema {
    public WFRunVariableTypeEnum type;
    public byte[] serializedVal;

    @JsonIgnore
    public Object getValue() {
        switch (type) {
        case STRING:
            return new String(serializedVal);
        case BOOLEAN:
            return Boolean.parseBoolean(new String(serializedVal));
        case INT:
            return Integer.valueOf(new String(serializedVal));
        case FLOAT:
            return Float.valueOf(new String(serializedVal));
        case ARRAY:
            try {
                return LHUtil.getObjectMapper(config).readValue(
                    serializedVal, List.class
                );
            } catch (IOException exn) {
                return null;
            }
        case OBJECT:
            try {
                return LHUtil.getObjectMapper(config).readValue(
                    serializedVal, Map.class
                );
            } catch (IOException exn) {
                return null;
            }
        default:
            LHUtil.logError("Uh oh....");
            return null;
        }
    }
}
