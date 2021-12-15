package little.horse.lib.schemas;

import little.horse.lib.LHStatus;

public class LHAPIResponsePost extends BaseSchema {
    public String name;
    public String guid;
    public LHStatus status;
}