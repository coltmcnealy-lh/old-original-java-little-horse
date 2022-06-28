package io.littlehorse.common.model.wfrun;

import io.littlehorse.common.exceptions.VarSubError;
import io.littlehorse.proto.WFRunEventPb;

public class WFRunEvent {
    private String wfRunId;
    private TaskStartedEvent startedEvent;
    private WFRunRequest runRequest;
    private TaskCompletedEvent completedEvent;

    public String getWFRunId() {
        return wfRunId;
    }

    public void setWFRunId(String wFRunId) {
        wfRunId = wFRunId;
    }

    public TaskStartedEvent getStartedEvent() {
        return startedEvent;
    }

    public void setStartedEvent(TaskStartedEvent startedEvent) {
        this.startedEvent = startedEvent;
    }

    public WFRunRequest getRunRequest() {
        return runRequest;
    }

    public void setRunRequest(WFRunRequest runRequest) {
        this.runRequest = runRequest;
    }

    public TaskCompletedEvent getCompletedEvent() {
        return completedEvent;
    }

    public void setCompletedEvent(TaskCompletedEvent completedEvent) {
        this.completedEvent = completedEvent;
    }

    public static WFRunEvent fromProto(WFRunEventPb proto) throws VarSubError {
        WFRunEvent out = new WFRunEvent();
        if (proto.hasCompletedEvent()) {
            out.setCompletedEvent(TaskCompletedEvent.fromProto(
                proto.getCompletedEvent()
            ));
        }
        if (proto.hasStartedEvent()) {
            out.setStartedEvent(TaskStartedEvent.fromProto(
                proto.getStartedEvent()
            ));
        }
        if (proto.hasRunRequest()) {
            out.runRequest = WFRunRequest.fromProto(
                proto.getRunRequest()
            );
        }
        return out;
    }

    public WFRunEventPb.Builder toProtoBuilder() {
        WFRunEventPb.Builder builder = WFRunEventPb.newBuilder().setWfRunId(wfRunId);
        if (completedEvent != null) {
            builder.setCompletedEvent(completedEvent.toProtoBuilder());
        }
        if (startedEvent != null) {
            builder.setStartedEvent(startedEvent.toProtoBuilder());
        }
        if (runRequest != null) {
            builder.setRunRequest(runRequest.toProtoBuilder());
        }
        return builder;
    }
}
