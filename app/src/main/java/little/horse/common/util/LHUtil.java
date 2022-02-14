package little.horse.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
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

import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.TaskQueue;
import little.horse.common.util.json.JsonMapKeyDeserializer;


public class LHUtil {
    public static String generateGuid() {
        return UUID.randomUUID().toString();
    }

    public static ObjectMapper getObjectMapper(Config cfg) {
        return new MapperInitializer(cfg).getMapper();
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
        ).toString();
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
        logBack(1, "ERROR:", things);
    }

    /**
     * Given an object, try to turn it into a HashMap (via json.loads). If possible
     * return the hashmap; otherwise return the variable.
     * @param obj the thing to unsplat
     * @return the unsplatted object
     */
    public static HashMap<String, Object> unsplat(Object obj, Config cfg) {
        HashMap<String, Object> out;
        try {
            LHUtil.log("obj:", obj.toString());

            @SuppressWarnings("unchecked")
            HashMap<String, Object> tmp = (HashMap<String, Object>) jsonifyIfPossible(
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

    public static String jsonify(Object thing, Config cfg) {
        try {
            return getObjectMapper(cfg).writeValueAsString(thing);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }

    public static Object jsonifyIfPossible(String data, Config cfg) {
        try {
            Object obj = LHUtil.getObjectMapper(cfg).readValue(data, Object.class);
            return obj;
        } catch(Exception exn) {
            return data;
        }
    }

    public static Object jsonPath(String json, String path) {
        return JsonPath.parse(json).read(path);
    }
}


class MapperInitializer {
    // Citations: https://stackoverflow.com/questions/42763298/jackson-keep-references-to-keys-in-map-values-when-deserializing
    private ObjectMapper mapper;

    public ObjectMapper getMapper() {
        return mapper;
    }

    public MapperInitializer(Config cfg) {
        this.mapper = new ObjectMapper();

        InjectableValues inject = new InjectableValues.Std().addValue(
            BaseSchema.class, cfg
        ).addValue(Config.class, cfg);

        mapper.setInjectableValues(inject);
        // SimpleModule module = new SimpleModule();
        // module.setDeserializerModifier(new BeanDeserializerModifier() {
        //     @Override
        //     public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, 
        //             BeanDescription beanDescription, JsonDeserializer<?> originalDeserializer) {
        //         return new JsonMapKeyDeserializer(originalDeserializer, beanDescription);
        //     }
        // });
        // mapper.registerModule(module);
    }
}
