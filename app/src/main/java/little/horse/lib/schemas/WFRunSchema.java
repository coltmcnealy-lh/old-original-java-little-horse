package little.horse.lib.schemas;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.streams.processor.api.Record;

import little.horse.lib.LHFailureReason;
import little.horse.lib.LHUtil;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.wfRuntime.WFRunStatus;

public class WFRunSchema extends BaseSchema {
    public String guid;
    public String wfSpecGuid;
    public String wfSpecName;
    public ArrayList<WFTokenSchema> tokens;

    public WFRunStatus status;
    public HashMap<String, Object> variables;

    public LHFailureReason errorCode;
    public String errorMessage;

    public HashMap<String, ArrayList<ExternalEventCorrelSchema>> pendingEvents;

    public boolean handleExternalEvent(
        WFEventSchema event,
        WFRunSchema wfRun,
        final Record<String, WFEventSchema> record,
        WFSpec wfSpec
    ) {
        return false;
    }

    public boolean handleWorkflowProcessingFailed(
        WFEventSchema event,
        WFRunSchema wfRun,
        final Record<String, WFEventSchema> record,
        WFSpec wfSpec
    ) {
        return false;
    }

    public static WFRunSchema handleWFRunStarted(
        WFEventSchema event,
        WFRunSchema wfRun,
        final Record<String, WFEventSchema> record,
        WFSpec wfSpec
    ) {
        return null;
    }

    public void addNewToken(TaskRunSchema rootTaskRun, WFTokenSchema parent) {
        // TODO
        LHUtil.log("here in addNewToken");
    }
}
