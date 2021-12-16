package little.horse.lib.schemas;

import java.util.Date;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import little.horse.lib.WFEventType;

public class WFEventSchema extends BaseSchema {
    public String wfSpecGuid;
    public String wfSpecName;
    public String wfRunGuid;
    public Date timestamp;

    public WFEventType type;

    // In the case of a Task Run, for example, this is just a serialized TaskRunSchema
    // object. For other things, such as external events, it'll be other things.
    public String content;

    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
