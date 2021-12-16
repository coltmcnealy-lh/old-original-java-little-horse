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

    public static void log(Object... things) {
        System.out.print(Thread.currentThread().getStackTrace()[2].getMethodName());
        System.out.print(" ");
        System.out.print(Thread.currentThread().getStackTrace()[2].getFileName());
        System.out.print(": ");
        System.out.print(Thread.currentThread().getStackTrace()[2].getLineNumber());
        System.out.print(": ");
        for (Object thing : things) {
            System.out.print(thing.toString());
            System.out.print(" ");
        }
        System.out.println();
    }

    public static void logError(Object... things) {
        System.out.print(Thread.currentThread().getStackTrace()[2].getMethodName());
        System.out.print(" ");
        System.out.print(Thread.currentThread().getStackTrace()[2].getFileName());
        System.out.print(": ");
        System.out.print(Thread.currentThread().getStackTrace()[2].getLineNumber());
        System.out.print(": ");
        for (Object thing : things) {
            System.out.print(thing.toString());
            System.out.print(" ");
        }
        System.out.println();
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
}