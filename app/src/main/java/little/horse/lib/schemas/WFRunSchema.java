package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import little.horse.lib.LHFailureReason;
import little.horse.lib.wfRuntime.WFRunStatus;

public class WFRunSchema extends BaseSchema {
    public String guid;
    public String wfSpecGuid;
    public String wfSpecName;
    public ArrayList<ThreadRunSchema> threadRuns;

    public WFRunStatus status;

    public LHFailureReason errorCode;
    public String errorMessage;

    // Event Sourcing! Yay!
    public ArrayList<WFEventIDSchema> history;

    public HashMap<String, ArrayList<ExternalEventCorrelSchema>> correlatedEvents;
    public Stack<String> pendingInterrupts;

    public HashMap<String, ArrayList<ThreadRunMetaSchema>> awaitableThreads;
}
