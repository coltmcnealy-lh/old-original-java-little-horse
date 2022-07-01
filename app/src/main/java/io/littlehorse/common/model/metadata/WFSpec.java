package io.littlehorse.common.model.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import io.littlehorse.common.model.BaseSchema;


// Just scoping for the purposes of the json parser
@JsonIdentityInfo(
    generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "name",
    scope = WFSpec.class
)
public class WFSpec extends BaseSchema {
    @JsonIgnore
    public static String typeName = "wfSpec";

    public String id;
    public String name;

    // Actual definition here.
    @JsonManagedReference
    public List<ThreadSpec> threadSpecs;

    public HashSet<String> interruptEvents;
    public String entrypointThreadName;

    // Internal bookkeeping for validation
    @JsonIgnore
    private HashMap<String, HashMap<String, WFRunVariableDef>> allVarDefs;

    @JsonIgnore
    public ThreadSpec findThreadSpec(String name) {
        for (ThreadSpec tspec: threadSpecs) {
            if (tspec.name.equals(name)) {
                return tspec;
            }
        }
        return null;
    }
}
