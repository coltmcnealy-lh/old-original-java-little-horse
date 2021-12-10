package little.horse.lib;

import java.util.ArrayList;
import java.util.HashMap;

public class WFSpecSchema {
    public String name;
    public String guid;
    public HashMap<String, NodeSchema> nodes;
    public ArrayList<EdgeSchema> edges;
    public LHStatus status;
    public String kafkaTopic;
    public String entrypointNodeName;
    public LHStatus desiredStatus;
}
