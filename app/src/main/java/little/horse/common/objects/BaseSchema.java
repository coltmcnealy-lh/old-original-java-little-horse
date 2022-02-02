package little.horse.common.objects;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;

import little.horse.common.Config;
import little.horse.common.exceptions.LHNoConfigException;
import little.horse.common.util.LHUtil;

public class BaseSchema {
    @JsonIgnore
    protected Config config;

    public BaseSchema() {
    }

    public BaseSchema(Config config) {
        this.config = config;
    }

    @JsonIgnore
    public Config setConfig(Config config) {
        if (config != null) {
            this.config = config;
        }
        return this.config;
    }

    /**
     * Passes the config object between two BaseSchema's. There should only be one
     * singleton config in the whole system, but I don't like dependency injection,
     * so we pass it along like the Heracles in James Bond No Time To Die.
     * @param other the BaseSchema to pass the Heracles to.
     */
    public void passConfig(BaseSchema other) {
        if (other.config == null) {
            other.setConfig(config);
        } else if (config == null) {
            config = other.config;
        }
    }

    @JsonIgnore
    public Config getConfig() throws LHNoConfigException {
        if (config == null) throw new LHNoConfigException();
        return config;
    }

    public static <T> T fromString(String src, Class<? extends BaseSchema> valueType) {
        Object result;
        try {
            result = LHUtil.mapper.readValue(src, valueType);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            // John Hanley question: Should we throw an OrzDash here instead of returning null?
            return null;
        }
        if (result.getClass() == valueType) {
            @SuppressWarnings("unchecked") T out = (T) result;
            return out;
        }
        
        return null;
    }

    public static <T> T fromBytes(byte[] src, Class<? extends BaseSchema> valueType) {
        Object result;
        try {
            result = LHUtil.mapper.readValue(src, valueType);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();

            // John Hanley question: Should we throw an OrzDash here instead of returning null?
            return null;
        } catch(IOException exn) {
            exn.printStackTrace();
            return null;
        }
        if (result.getClass() == valueType) {
            @SuppressWarnings("unchecked") T out = (T) result;
            return out;
        }
        
        return null;
    }

    public String toString() {
        try {
            return LHUtil.mapper.writeValueAsString(this);
        } catch(Exception exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
