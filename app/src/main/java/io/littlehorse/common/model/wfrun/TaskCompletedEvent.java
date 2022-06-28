package io.littlehorse.common.model.wfrun;

import java.sql.Date;
import java.time.Instant;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.littlehorse.proto.TaskCompletedEventPb;

public class TaskCompletedEvent {
    private int threadRunNumber;
    private int taskRunNumber;
    private int taskRunPosition;
    private Date time;
    private String workerId;
    private byte[] output;
    private byte[] logOutput;
    private boolean success;

    public byte[] getOutput() {
        return output;
    }

    public void setOutput(byte[] output) {
        this.output = output;
    }

    public byte[] getLogOutput() {
        return logOutput;
    }

    public void setLogOutput(byte[] logOutput) {
        this.logOutput = logOutput;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
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

    public static TaskCompletedEvent fromProto(TaskCompletedEventPb proto) {
        TaskCompletedEvent out = new TaskCompletedEvent();
        out.setThreadRunNumber(proto.getThreadRunNumber());
        out.setTaskRunNumber(proto.getTaskRunNumber());
        out.setTaskRunPosition(proto.getTaskRunPosition());

        out.setTime(new Date(Instant.ofEpochSecond(
            proto.getTime().getSeconds(),
            proto.getTime().getNanos()).toEpochMilli()
        ));

        out.setWorkerId(proto.getWorkerId());
        out.setOutput(proto.getOutput().toByteArray());
        out.setLogOutput(proto.getLogOutput().toByteArray());
        out.setSuccess(proto.getSuccess());
        return out;
    }

    public TaskCompletedEventPb.Builder toProtoBuilder() {
        TaskCompletedEventPb.Builder b = TaskCompletedEventPb.newBuilder();
        b.setThreadRunNumber(threadRunNumber);
        b.setTaskRunNumber(taskRunNumber);
        b.setTaskRunPosition(taskRunPosition);

        Instant i = time.toInstant();
        b.setTime(Timestamp.newBuilder().setSeconds(
            i.getEpochSecond()
        ).setNanos(i.getNano()));

        b.setWorkerId(workerId);
        b.setOutput(ByteString.copyFrom(output));
        b.setSuccess(success);
        return b;
    }
}
