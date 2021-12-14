package little.horse.lib;

import java.util.ArrayList;
import java.util.HashMap;

public class NodeSchema {
    public String name;
    public NodeType nodeType;
    public String taskDefinitionName;
    public String wfSpecGuid;
    public String guid;

    public ArrayList<WFTriggerSchema> triggers;
    public ArrayList<EdgeSchema> outgoingEdges;

    public HashMap<String, String> variables;

    // LATER: Maybe add inputVariables, which would be just an annotation
    // denoting what is going to come next.
}
