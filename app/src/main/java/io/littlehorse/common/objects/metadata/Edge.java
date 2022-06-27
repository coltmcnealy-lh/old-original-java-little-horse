package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.littlehorse.common.objects.BaseSchema;

public class Edge extends BaseSchema {
    public String sourceNodeName;
    public String sinkNodeName;
    public EdgeCondition condition;

    public boolean alreadyActivated = false;

    @JsonBackReference
    public ThreadSpec thread;

    @JsonIgnore
    private Node sourceNode;

    @JsonIgnore
    public Node getSourceNode() {
        if (sourceNode == null) {
            sourceNode = thread.findNode(sourceNodeName);
        }
        return sourceNode;
    }

    @JsonIgnore
    private Node sinkNode;

    @JsonIgnore
    public Node getSinkNode() {
        if (sinkNode == null) {
            sinkNode = thread.findNode(sinkNodeName);
        }
        return sinkNode;
    }
}
