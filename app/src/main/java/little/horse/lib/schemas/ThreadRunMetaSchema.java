package little.horse.lib.schemas;

public class ThreadRunMetaSchema extends BaseSchema {
    public int threadID;
    public int parentThreadID;
    public String threadSpecName;

    public String sourceNodeName;
    public String sourceNodeGuid;

    // No info about thread status or result present here because that
    // can be looked up from the threadID without another network hop.

    public int timesAwaited = 0;
}
