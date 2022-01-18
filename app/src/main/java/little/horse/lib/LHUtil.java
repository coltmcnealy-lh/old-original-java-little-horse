package little.horse.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import org.apache.commons.lang3.StringUtils;

public class LHUtil {
    public static String generateGuid() {
        return UUID.randomUUID().toString();
    }

    public static final ObjectMapper mapper = new ObjectMapper();

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

    public static String digestify(String str) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch(NoSuchAlgorithmException exn) {
            // LOL shouldn't happen
        }
        byte[] encodedhash = digest.digest(str.getBytes(StandardCharsets.UTF_8));
        return encodedhash.toString().substring(0, 8);
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

    public static String jsonify(Object thing) {
        try {
            return mapper.writeValueAsString(thing);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }

    public static Object jsonifyIfPossible(String data) {
        try {
            Object obj = LHUtil.mapper.readValue(data, Object.class);
            return obj;
        } catch(Exception exn) {
            return data;
        }
    }

    public static Object jsonPath(String json, String path) {
        return JsonPath.parse(json).read(path);
    }
}