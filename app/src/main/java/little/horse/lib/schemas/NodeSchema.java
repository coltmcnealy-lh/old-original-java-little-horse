package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.NodeType;

public class NodeSchema extends BaseSchema {
    public String name;
    public NodeType nodeType;
    public String taskDefinitionName;
    public String wfSpecGuid;
    public String guid;

    public ArrayList<WFTriggerSchema> triggers;
    public ArrayList<EdgeSchema> outgoingEdges;

    public HashMap<String, VariableDefinitionSchema> variables;

    // LATER: Maybe add inputVariables, which would be just an annotation
    // denoting what is going to come next.
}
