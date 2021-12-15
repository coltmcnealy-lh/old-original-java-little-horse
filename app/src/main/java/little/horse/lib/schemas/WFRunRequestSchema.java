package little.horse.lib.schemas;

import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WFRunRequestSchema extends BaseSchema {
    public HashMap<String, Object> variables;
    public HashMap<String, Object> inputVariables;

    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
