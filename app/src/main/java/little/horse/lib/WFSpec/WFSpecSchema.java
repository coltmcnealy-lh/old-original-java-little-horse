package little.horse.lib.WFSpec;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.LHStatus;

public class WFSpecSchema {
    public String name;
    public String guid;
    public HashMap<String, NodeSchema> nodes;
    public ArrayList<EdgeSchema> edges;
    public LHStatus status;
    public String inputKafkaTopic;
    public String entrypointNodeName;
    public LHStatus desiredStatus;
}
