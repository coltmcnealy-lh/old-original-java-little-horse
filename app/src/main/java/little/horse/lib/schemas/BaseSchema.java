package little.horse.lib.schemas;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;

import little.horse.lib.LHUtil;

public class BaseSchema {

    public static <T> T fromString(String src, Class<? extends BaseSchema> valueType) {
        Object result;
        try {
            result = LHUtil.mapper.readValue(src, valueType);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            System.out.println("Failed jsonloading this: " + src);

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
            System.out.println("Failed jsonloading this: " + src);
            System.out.println(new String(src));

            // John Hanley question: Should we throw an OrzDash here instead of returning null?
            return null;
        } catch(IOException exn) {
            exn.printStackTrace();
            System.out.println("Failed jsonloading this: " + src);
            System.out.println(new String(src));
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
