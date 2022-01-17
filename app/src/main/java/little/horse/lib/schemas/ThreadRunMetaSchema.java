package little.horse.lib.schemas;

public class ThreadRunMetaSchema extends BaseSchema {
    public int threadID;
    public String wfRunGuid;
    public String wfSpecGuid;
    public String wfSpecName;

    public String sourceNodeName;
    public String sourceNodeGuid;

    // No info about thread status or result present here because that
    // can be looked up from the threadID without another network hop.

    public int timesAwaited = 0;
}
