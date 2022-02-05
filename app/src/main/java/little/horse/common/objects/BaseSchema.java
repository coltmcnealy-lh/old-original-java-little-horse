package little.horse.common.objects;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
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
        this.config = config;
        return config;
    }

    @DigestIgnore
    private String digest;

    @SuppressWarnings("unchecked")
    public String getDigest() {
        if (digest != null) return digest;

        HashMap<String, String> digests = new HashMap<>();

        for (Field field: this.getClass().getDeclaredFields()) {
            if (
                field.isAnnotationPresent(DigestIgnore.class)
                || field.isAnnotationPresent(JsonIgnore.class)
            ) {
                continue;
            }

            field.setAccessible(true);
            String key = field.getName();

            try {
                Object obj = field.get(this);
                if (obj == null) {
                    digests.put(key, "");
                } else if (obj instanceof BaseSchema) {
                    digests.put(key, ((BaseSchema)obj).getDigest());
                } else if (obj instanceof List) {
                    List<String> thingStrings = new ArrayList<String>();
                    for (Object thing: (List<Object>) obj) {
                        if (thing instanceof BaseSchema) {
                            thingStrings.add(((BaseSchema)thing).getDigest());
                        } else {
                            thingStrings.add(thing.toString());
                        }
                        digests.put(key, thingStrings.toString());
                    }
                } else if (obj instanceof Map) {
                    Map<String, String> thingMap = new HashMap<>();

                    for (Map.Entry<Object, Object> entry: 
                        ((Map<Object, Object>)obj).entrySet()
                    ) {
                        String val;
                        if (entry.getValue() instanceof BaseSchema) {
                            val = ((BaseSchema) entry.getValue()).getDigest();
                        } else {
                            val = entry.getValue().toString();
                        }

                        thingMap.put(entry.getKey().toString(), val);
                    }

                    digests.put(key, thingMap.toString());
                } else {
                    digests.put(key, field.get(this).toString());
                }
            } catch (IllegalAccessException exn) {
                LHUtil.logError("Shouldn't be possible", exn);
                exn.printStackTrace();
            } catch (ClassCastException exn) {
                exn.printStackTrace();
            }
        }
        digest = LHUtil.fullDigestify(digests.toString());
        return digest;
    }

    public static <T> T fromString(
        String src, Class<? extends BaseSchema> valueType,
        Config config, boolean validate
    ) {
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

    public static <T> T fromBytes(
        byte[] src, Class<? extends BaseSchema> valueType,
        Config config, boolean validate
    ) {
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

    public void fillOut(Config config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        // Nothing to do.
    }

    public boolean filledOut = false;

    public void linkUp() {
        // Nothing to do for general case; can be overriden.
    }

    public void validate() {
        // Nothing to do for general case; can be overriden.
    }
}
