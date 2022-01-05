package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.LHStatus;

public class WFSpecSchema extends BaseSchema {
    public String name;
    public String guid;
    public HashMap<String, NodeSchema> nodes;
    public ArrayList<EdgeSchema> edges;
    public LHStatus status;
    public String kafkaTopic;
    public String entrypointNodeName;
    public LHStatus desiredStatus;

    public HashMap<String, WFRunVariableDefSchema> variableDefs;

    public ArrayList<SignalHandlerSpecSchema> signalHandlers;
}
