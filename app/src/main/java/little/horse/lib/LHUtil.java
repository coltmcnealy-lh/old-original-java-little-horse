package little.horse.lib;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;

public class LHUtil {
    public static String generateGuid() {
        return UUID.randomUUID().toString();
    }

    public static final ObjectMapper mapper = new ObjectMapper();

    public static String toValidK8sName(String oldStr) {
        String str = new String(oldStr);

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

        }
        byte[] encodedhash = digest.digest(str.getBytes(StandardCharsets.UTF_8));
        return encodedhash.toString().substring(0, 8);
    }

    public Date now() {
        return new Date();
    }
}