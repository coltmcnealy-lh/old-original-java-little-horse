package little.horse.api;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class OffsetInfoCollection extends BaseSchema {
    public HashMap<String, OffsetInfo> partitionMap;

    public OffsetInfo latest;
}
