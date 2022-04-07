package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.DigestIgnore;

public class Edge extends BaseSchema {
    public String sourceNodeName;
    public String sinkNodeName;
    public EdgeCondition condition;

    @DigestIgnore
    public boolean alreadyActivated = false;

    @JsonBackReference
    public ThreadSpec thread;

    @JsonIgnore
    private Node sourceNode;

    @JsonIgnore
    public Node getSourceNode() {
        if (sourceNode == null) {
            sourceNode = thread.nodes.get(sourceNodeName);
        }
        return sourceNode;
    }

    @JsonIgnore
    private Node sinkNode;

    @JsonIgnore
    public Node getSinkNode() {
        if (sinkNode == null) {
            sinkNode = thread.nodes.get(sinkNodeName);
        }
        return sinkNode;
    }
}
