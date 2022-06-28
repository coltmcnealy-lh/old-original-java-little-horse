package io.littlehorse.common.model.wfrun;

import java.time.Instant;
import java.util.Date;
import com.google.protobuf.Timestamp;
import io.littlehorse.proto.TaskStartedEventPb;

public class TaskStartedEvent {
    private int threadRunNumber;
    private int taskRunNumber;
    private int taskRunPosition;
    private Date time;
    private String workerId;

    public static TaskStartedEvent fromProto(TaskStartedEventPb proto) {
        TaskStartedEvent out = new TaskStartedEvent();
        out.setThreadRunNumber(proto.getThreadRunNumber());
        out.setTaskRunNumber(proto.getTaskRunNumber());
        out.setTaskRunPosition(proto.getTaskRunPosition());

        out.setTime(new Date(Instant.ofEpochSecond(
            proto.getTime().getSeconds(),
            proto.getTime().getNanos()).toEpochMilli()
        ));

        out.setWorkerId(proto.getWorkerId());
        return out;
    }

    public TaskStartedEventPb.Builder toProtoBuilder() {
        TaskStartedEventPb.Builder b = TaskStartedEventPb.newBuilder();
        b.setThreadRunNumber(threadRunNumber);
        b.setTaskRunNumber(taskRunNumber);
        b.setTaskRunPosition(taskRunPosition);

        Instant i = time.toInstant();
        b.setTime(Timestamp.newBuilder().setSeconds(
            i.getEpochSecond()
        ).setNanos(i.getNano()));

        b.setWorkerId(workerId);
        return b;
    }

    public int getThreadRunNumber() {
        return threadRunNumber;
    }
    public void setThreadRunNumber(int threadRunNumber) {
        this.threadRunNumber = threadRunNumber;
    }
    public int getTaskRunNumber() {
        return taskRunNumber;
    }
    public void setTaskRunNumber(int taskRunNumber) {
        this.taskRunNumber = taskRunNumber;
    }
    public int getTaskRunPosition() {
        return taskRunPosition;
    }
    public void setTaskRunPosition(int taskRunPosition) {
        this.taskRunPosition = taskRunPosition;
    }
    public Date getTime() {
        return time;
    }
    public void setTime(Date time) {
        this.time = time;
    }
    public String getWorkerId() {
        return workerId;
    }
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
}
