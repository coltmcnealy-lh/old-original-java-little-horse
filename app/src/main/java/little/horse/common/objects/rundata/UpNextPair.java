package little.horse.common.objects.rundata;

import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.Edge;

public class UpNextPair extends BaseSchema {
    public int attemptNumber = 0;
    public Edge edge;

    public UpNextPair() {}

    public UpNextPair(int attemptNumber, Edge edge) {
        this.attemptNumber = attemptNumber;
        this.edge = edge;
    }
}
