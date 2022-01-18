package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;

public class ThreadSpecSchema extends BaseSchema {
    public String name;
    public String guid;

    public HashMap<String, WFRunVariableDefSchema> variableDefs;
    @JsonManagedReference
    public HashMap<String, NodeSchema> nodes;
    public ArrayList<EdgeSchema> edges;

    public String entrypointNodeName;

    @JsonBackReference
    public WFSpecSchema wfSpec;
}
