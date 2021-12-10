package little.horse.lib;

import java.util.ArrayList;

public class NodeSchema {
    public String name;
    public NodeType nodeType;
    public String taskDefinitionName;
    public String wfSpecGuid;
    public String guid;

    public ArrayList<WFTriggerSchema> triggers;

    // LATER: Maybe add inputVariables, which would be just an annotation
    // denoting what is going to come next.
}
