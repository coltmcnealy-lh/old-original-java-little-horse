package little.horse.common.objects;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.CoreMetadata;
import little.horse.common.util.LHUtil;

public class BaseSchema {
    @JacksonInject
    protected DepInjContext config;

    public BaseSchema(DepInjContext config) {
        this.config = config;
    }
    // Use sparingly.
    public BaseSchema(){}

    @JsonIgnore
    private String digest;

    public String getId() {
        return LHUtil.fullDigestify(getDigest());
    }
    @JsonIgnore private String id;
    // public void setId(String foo) {} // just here for jackson stupidity


    @SuppressWarnings("unchecked")
    @JsonIgnore
    private String getDigest() {
        if (digest != null) return digest;

        HashMap<String, String> digests = new HashMap<>();

        if (this instanceof CoreMetadata) {
            CoreMetadata thisCm = (CoreMetadata) this;
            digests.put("name", thisCm.name);
        }

        for (Field field: this.getClass().getDeclaredFields()) {
            if (
                field.isAnnotationPresent(DigestIgnore.class)
                || field.isAnnotationPresent(JsonIgnore.class)
                || field.isAnnotationPresent(JsonBackReference.class)
            ) {
                continue;
            }

            // Skip if not public.
            if (!Modifier.isPublic(field.getModifiers())) {
                continue;
            }

            String key = field.getName();

            try {
                Object obj = field.get(this);
                if (obj == null) {
                    digests.put(key, "");
                } else if (obj instanceof BaseSchema) {
                    digests.put(key, ((BaseSchema)obj).getId());
                } else if (obj instanceof List) {
                    List<String> thingStrings = new ArrayList<String>();
                    for (Object thing: (List<Object>) obj) {
                        if (thing instanceof BaseSchema) {
                            thingStrings.add(((BaseSchema)thing).getId());
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
                            val = ((BaseSchema) entry.getValue()).getId();
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
        digest = digests.toString();
        return digest;
    }

    public static <T extends BaseSchema> T fromString(
        String src, Class<T> valueType, DepInjContext config
    ) throws LHSerdeError {
        return BaseSchema.fromBytes(
            src.getBytes(StandardCharsets.UTF_8), valueType, config
        );
    }

    public static <T extends BaseSchema> T fromBytes(
        byte[] src, Class<T> valueType, DepInjContext config
    ) throws LHSerdeError {
        T result;
        try {
            result = LHUtil.getObjectMapper(config).readValue(src, valueType);
        } catch(JsonProcessingException parent) {
            parent.printStackTrace();
            throw new LHSerdeError(
                parent, "Failed to process json: " + parent.getMessage()
            );
        } catch(IOException exn) {
            exn.printStackTrace();
            throw new LHSerdeError(exn, "Had an IO Exception, ooph");
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

    public void validate(DepInjContext config) throws LHValidationError, LHConnectionError {
        setConfig(config);
        // Nothing to do in general case; should be overriden.
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    final public void setConfig(DepInjContext config) {
        // Needed to avoid infinite recursion. So 
        if (this.config != null) return;
        this.config = config;

        // Now set config of all the children.
        for (Field field: this.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object obj;
            try {
                obj = field.get(this);
            } catch (IllegalAccessException exn) {
                exn.printStackTrace();
                continue;
            }
            if (obj == null) continue;

            if (obj instanceof BaseSchema) {
                BaseSchema bs = (BaseSchema) obj;
                bs.setConfig(config);
            } else if (obj instanceof List) {
                for (Object subObj : (List<Object>) obj) {
                    if (subObj instanceof BaseSchema) {
                        ((BaseSchema) subObj).setConfig(config);
                    }
                }
            } else if (obj instanceof Map) {
                for (Object subObj: ((Map<Object, Object>) obj).values()) {
                    if (subObj instanceof BaseSchema) {
                        ((BaseSchema) subObj).setConfig(config);
                    }
                }
            }
        }
    }

    public byte[] toBytes() {
        return toString().getBytes();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (!(o instanceof BaseSchema && o.getClass().equals(this.getClass()))) {
            return false;
        }

        return ((BaseSchema)o).getId().equals(this.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
