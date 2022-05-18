package little.horse.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.hash.Hashing;
import com.jayway.jsonpath.JsonPath;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;

import io.javalin.Javalin;
import little.horse.common.DepInjContext;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFRunVariableDef;
import little.horse.common.objects.rundata.LHFailureReason;
import little.horse.common.objects.rundata.VarSubOrzDash;
import little.horse.common.util.json.JsonMapKeyDeserializer;


public class LHUtil {
    private static ObjectMapper mapper = null;

    public static String generateGuid() {
        return UUID.randomUUID().toString();
    }

    public static ObjectMapper getObjectMapper(DepInjContext cfg) {
        if (mapper == null) {
            mapper = new MapperInitializer(cfg).getMapper();
        }
        return mapper;
    }

    public static String toValidK8sName(String oldStr) {
        String str = new String(oldStr);
        str = str.toLowerCase();

        // TODO: This is probably not right.
        str = str.replaceAll("[. _\n]", "-");
        str = str.replaceAll("[^0-9a-z-]", "");
        str = str.replaceAll("-[-]+", "-");
        str = StringUtils.stripStart(str, "-");
        str = StringUtils.stripEnd(str, "-");
        if (str.length() >= 63) {
            str = str.substring(0, 54) + "-" + digestify(str);
        }
        return str;
    }

    public static String fullDigestify(String str) {
        return Hashing.sha256().hashString(
            str, StandardCharsets.UTF_8
        ).toString().substring(0, 18);
    }

    public static String digestify(String str) {
        return fullDigestify(str).substring(0, 8);
    }

    public static Date now() {
        return new Date();
    }

    /**
     * Logs stuff.
     * @param framesBack The number of stack frames back to go 
     * @param things
     */
    public static void logBack(int framesBack, Object... things) {
        framesBack += 2;  // 2 frames needed for processing the thing.
        StackTraceElement ste = Thread.currentThread().getStackTrace()[framesBack];

        System.out.print("LHorse: ");
        System.out.print(ste.getMethodName());
        System.out.print(" ");
        System.out.print(ste.getFileName());
        System.out.print(": ");
        System.out.print(ste.getLineNumber());
        System.out.print(": ");
        for (Object thing : things) {
            System.out.print(thing == null ? "null" : thing.toString());
            System.out.print(" ");
        }
        System.out.println();
    }

    public static void log(Object... things) {
        logBack(1, things);  // Add one frame back because of this method call.
    }

    public static void logError(Object... things) {
        logBack(1, things);
    }

    /**
     * Given an object, try to turn it into a HashMap (via json.loads). If possible
     * return the hashmap; otherwise return the variable.
     * @param obj the thing to unsplat
     * @return the unsplatted object
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> unsplat(Object obj, DepInjContext cfg) {
        HashMap<String, Object> out;
        try {
            LHUtil.log("obj:", obj.toString());
            if (obj instanceof Map) {
                return (Map<String, Object>) Map.class.cast(obj);
            }

            HashMap<String, Object> tmp = (HashMap<String, Object>) stringToObj(
                obj.toString(), cfg
            );

            out = tmp;
        } catch (Exception exn) {
            out = new HashMap<>();
            out.put("", obj);
        }
        return out;
    }

    public static String inputStreamToString(InputStream stream) {
        int bufferSize = 1024;
        char[] buffer = new char[bufferSize];
        StringBuilder out = new StringBuilder();
        Reader in = new InputStreamReader(stream);
        try {
            for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
                out.append(buffer, 0, numRead);
            }
        } catch(IOException exn) {
            exn.printStackTrace();
        }
        return out.toString();
    }

    public static String toJsonString(Object thing, DepInjContext cfg) {
        try {
            return getObjectMapper(cfg).writeValueAsString(thing);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }

    public static Object stringToObj(String data, DepInjContext cfg) {
        try {
            Object obj = LHUtil.getObjectMapper(cfg).readValue(data, Object.class);
            return obj;
        } catch(Exception exn) {
            // LHUtil.logBack(
            //     1,
            //     "LHUtil.fromStringToObj() caught exception", exn.getMessage()
            // );
            return data;
        }
    }

    public static Object jsonPath(String json, String path) throws VarSubOrzDash {
        try {
            return JsonPath.parse(json).read(path);
        } catch (Exception exn) {
            throw new VarSubOrzDash(
                exn,
                "Failed accessing path " + path + " on data " + json + "  :\n" +
                exn.getMessage()
            );
        }
    }

    public static String objToString(Object thing) {
        if (thing == null) return "null";
        if (thing instanceof Map) {
            try {
                if (LHUtil.mapper != null) {
                    return LHUtil.mapper.writeValueAsString(thing);
                }
                return new ObjectMapper().writeValueAsString(thing);
            } catch (Exception exn) {
                exn.printStackTrace();
                return null;
            }
        }
        return thing.toString();
    }

    @SuppressWarnings("unchecked")
    public static Object lhCopy(Object o) {
        if (o instanceof Integer) {
            return Integer.valueOf((Integer) o);
        } else if (o instanceof Double) {
            return Double.valueOf((Double) o);
        } else if (o instanceof Boolean) {
            return Boolean.valueOf((Boolean) o);
        } else if (o instanceof String) {
            return new String((String) o);
        } else if (o instanceof List) {
            ArrayList<Object> out = new ArrayList<>();
            for (Object obj : (List<Object>) o) {
                out.add(LHUtil.lhCopy(obj));
            }
            return out;
        } else if (o instanceof Map) {
            Map<Object, Object> m = new HashMap<>();
            Map<Object, Object> oMap = (Map<Object, Object>) o;
            for (Object k: oMap.keySet()) {
                m.put(LHUtil.lhCopy(k), LHUtil.lhCopy(oMap.get(k)));
            }
            return m;
        } else if (o == null) {
            return null;
        } else {
            LHUtil.log(o, o.getClass());
            throw new RuntimeException("Invalid type for LHCopy!");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Object> T loadClass(String className) {
        Class<T> cls;
        try {
            cls = (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException exn) {
            throw new LHClassLoadError(
                "Unable to find provided classname " + className + ": "
                + exn.getMessage(), exn
            );
        }

        try {
            return cls.cast(
                cls.getDeclaredConstructor().newInstance()
            );
        } catch(IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException 
                | InstantiationException exn) {
            throw new LHClassLoadError(
                "Unable to instantiate Object of type " + className + ": " +
                exn.getMessage(), exn
            );
        }
    }

    public static Javalin createAppWithHealth(KStreamsStateListener listener) {
        Javalin app = Javalin.create(javalinConf -> {
            javalinConf.prefer405over404 = true;
            javalinConf.enableCorsForAllOrigins();
        });

        app.get("/health", (ctx) -> {
            if (listener.getState() == KafkaStreams.State.RUNNING) {
                ctx.status(200);
                ctx.result("OK");
            } else {
                ctx.status(500);
                ctx.result(listener.getState().toString());
            }
        });
        return app;
    }

    public static Class<? extends Object> getNeededClass(WFRunVariableDef vardef) {
        switch (vardef.type) {
            case STRING:    return String.class;
            case INT:       return Integer.class;
            case FLOAT:    return Double.class;
            case ARRAY:     return List.class;
            case OBJECT:    return Object.class;
            case BOOLEAN:   return Boolean.class;
            default: throw new RuntimeException("Not possible");
        }
    }

    /**
     * Returns whether or not TaskRun failures called by this failure are retryable.
     * TODO: Maybe put this in somewhere that makes more sense rather than LHUtil.
     * @param reason the LHFailureReason from task failure.
     * @return True if we can retry task failures from that reason.
     */
    public static boolean isRetryable(LHFailureReason reason) {
        switch (reason) {
            case TASK_FAILURE:                  return true;
            case VARIABLE_LOOKUP_ERROR:         return false;
            case INVALID_WF_SPEC_ERROR:         return false;
            case TIMEOUT:                       return true;
            case INTERNAL_LITTLEHORSE_ERROR:    return true;
            case SUBTHREAD_FAILURE:             return false;
            default: throw new RuntimeException("Not possible.");
        }
    }
}


class MapperInitializer {
    // Citations: https://stackoverflow.com/questions/42763298/jackson-keep-references-to-keys-in-map-values-when-deserializing
    private ObjectMapper mapper;

    public ObjectMapper getMapper() {
        return mapper;
    }

    public MapperInitializer(DepInjContext cfg) {
        this.mapper = new ObjectMapper();

        InjectableValues inject = new InjectableValues.Std().addValue(
            BaseSchema.class, cfg
        ).addValue(DepInjContext.class, cfg);

        mapper.setInjectableValues(inject);
        SimpleModule module = new SimpleModule();
        module.setDeserializerModifier(new BeanDeserializerModifier() {
            @Override
            public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, 
                    BeanDescription beanDescription, JsonDeserializer<?> originalDeserializer) {
                return new JsonMapKeyDeserializer(originalDeserializer, beanDescription);
            }
        });
        mapper.registerModule(module);
    }
}
