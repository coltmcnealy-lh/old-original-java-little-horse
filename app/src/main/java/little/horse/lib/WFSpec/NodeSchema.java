package little.horse.lib.WFSpec;

import java.util.ArrayList;

public class NodeSchema {
    public String name;
    public NodeType nodeType;
    public String taskDefinitionName;
    public String wfSpecGuid;
    public String externalEventDefinitionName;
    public String guid;

    public String outputKafkaTopic;
    public ArrayList<String> inputKafkaTopics;
    public String interruptKafkaTopic;

    public String externalEventCorrelation;

    // LATER: Maybe add inputVariables, which would be just an annotation
    // denoting what is going to come next.
}
