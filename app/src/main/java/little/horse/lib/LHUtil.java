package little.horse.lib;

import java.util.UUID;

public class LHUtil {
    public static String generateGuid() {
        return UUID.randomUUID().toString();
    }
}