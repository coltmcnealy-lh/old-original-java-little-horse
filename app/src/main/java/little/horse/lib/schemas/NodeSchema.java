package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import little.horse.lib.NodeType;

public class NodeSchema extends BaseSchema {
    public String name;
    public NodeType nodeType;
    public String taskDefinitionName;
    public String wfSpecGuid;
    public String threadSpecName;
    public String guid;

    public ArrayList<EdgeSchema> outgoingEdges;
    public ArrayList<EdgeSchema> incomingEdges;

    public HashMap<String, VariableAssignmentSchema> variables;

    public String externalEventDefName;
    public String externalEventDefGuid;

    public HashMap<String, VariableMutationSchema> variableMutations;
}
