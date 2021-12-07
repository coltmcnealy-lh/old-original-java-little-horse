package little.horse.lib;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

public class LHUtil {
    public static String generateGuid() {
        return UUID.randomUUID().toString();
    }

    public static String toValidK8sName(String str) {
        // TODO: This is probably not right.
        str = str.replaceAll("[. _\n]", "-");
        str = str.replaceAll("[^0-9a-z-]", "");
        str = str.replaceAll("-[-]+", "-");
        str = StringUtils.stripStart(str, "-");
        str = StringUtils.stripEnd(str, "-");
        if (str.length() >= 63) {
            MessageDigest digest = null;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch(NoSuchAlgorithmException exn) {

            }
            byte[] encodedhash = digest.digest(str.getBytes(StandardCharsets.UTF_8));
            String token = "-" + encodedhash.toString().substring(0, 8);
            str = str.substring(0, 54) + token;
        }
        return str;
    }
}