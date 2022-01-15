package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

public class ThreadSpecSchema {
    public String name;
    public String guid;

    public HashMap<String, WFRunVariableDefSchema> variableDefs;
    public HashMap<String, NodeSchema> nodes;
    public ArrayList<EdgeSchema> edges;

    public String entrypointNodeName;
}
