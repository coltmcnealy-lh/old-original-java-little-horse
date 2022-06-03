package io.littlehorse.common.objects.rundata;

import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.Edge;

public class UpNextPair extends BaseSchema {
    public int attemptNumber = 0;
    public Edge edge;

    public UpNextPair() {}

    public UpNextPair(int attemptNumber, Edge edge) {
        this.attemptNumber = attemptNumber;
        this.edge = edge;
    }
}
