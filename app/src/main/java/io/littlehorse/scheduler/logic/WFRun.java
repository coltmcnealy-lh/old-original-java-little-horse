package io.littlehorse.scheduler.logic;

import java.util.ArrayList;
import java.util.List;
import com.google.protobuf.Timestamp;
import io.littlehorse.common.exceptions.VarSubError;
import io.littlehorse.common.model.metadata.Edge;
import io.littlehorse.common.model.metadata.EdgeCondition;
import io.littlehorse.common.model.metadata.Node;
import io.littlehorse.common.model.metadata.ThreadSpec;
import io.littlehorse.common.model.metadata.VariableAssignment;
import io.littlehorse.common.model.metadata.VariableValue;
import io.littlehorse.common.model.metadata.WFSpec;
import io.littlehorse.proto.LHStatusPb;
import io.littlehorse.proto.TaskRunPb;
import io.littlehorse.proto.ThreadRunPb;
import io.littlehorse.proto.VariableValuePbOrBuilder;
import io.littlehorse.proto.WFRunEventPbOrBuilder;
import io.littlehorse.proto.WFRunPb;
import io.littlehorse.proto.WFRunPbOrBuilder;
import io.littlehorse.proto.WFRunRequestPb;
import io.littlehorse.scheduler.SchedulerOutput;

public class WFRun {
    public static List<SchedulerOutput> handleEvent(
        WFSpec spec, WFRunPbOrBuilder oldRun, WFRunEventPbOrBuilder evt
    ) {
        WFRunPb.Builder runBuilder;
        int threadToAdvance;
        List<SchedulerOutput> out = new ArrayList<>();

        if (evt.hasCompletedEvent()) {
            runBuilder = handleCompletedEvt(spec, oldRun, evt);
            threadToAdvance = evt.getCompletedEvent().getThreadRunNumber();

        } else if (evt.hasStartedEvent()) {
            runBuilder = handleStartedEvt(spec, oldRun, evt);
            threadToAdvance = evt.getStartedEvent().getThreadRunNumber();

        } else {
            if (oldRun != null) return out;
            runBuilder = handleRunRequest(spec, oldRun, evt);
            threadToAdvance = 0;

        }

        out = advanceThread(threadToAdvance, runBuilder, spec);
        out.add(new SchedulerOutput(runBuilder));
        return out;
    }

    private static WFRunPb.Builder handleCompletedEvt(
        WFSpec spec, WFRunPbOrBuilder oldRun, WFRunEventPbOrBuilder evt
    ) {
        return null;
    }

    private static WFRunPb.Builder handleStartedEvt(
        WFSpec spec, WFRunPbOrBuilder oldRun, WFRunEventPbOrBuilder evt
    ) {
        return null;
    }

    private static WFRunPb.Builder handleRunRequest(
        WFSpec spec, WFRunPbOrBuilder oldRun, WFRunEventPbOrBuilder evt
    ) {
        WFRunRequestPb runReq = evt.getRunRequest();
        if (oldRun != null) {
            // That means another wfrun exists with the same ID, so we don't want to
            // over-write it. In the future we should implement a mechanism to notify
            // the client that the WFRunRequest failed.
            return null;
        }

        long millis = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1000000)).build();

        ThreadSpec tspec = spec.findThreadSpec(spec.entrypointThreadName);

        ThreadRunPb.Builder threadBuilder = ThreadRunPb.newBuilder()
            .setStatus(LHStatusPb.RUNNING)
            .setThreadSpecName(tspec.name)
            .setCurrentNode(tspec.entrypointNodeName);

        WFRunPb.Builder runBuilder = WFRunPb.newBuilder()
            .setId(runReq.getWfRunId())
            .setStartTime(timestamp)
            .setWfSpecId(runReq.getWfSpecId())
            .setWfSpecName(spec.name)
            .setStatus(LHStatusPb.RUNNING)
            .addThreadRuns(threadBuilder);

        return runBuilder;
    }

    private static List<SchedulerOutput> advanceThread(
        int threadNum, WFRunPb.Builder wfRun, WFSpec spec
    ) {
        List<SchedulerOutput> out = new ArrayList<SchedulerOutput>();

        ThreadRunPb.Builder thread = wfRun.getThreadRunsBuilder(threadNum);
        if (thread == null || thread.getStatus() == LHStatusPb.COMPLETED) {
            return out;
        }

        String curNodeName = thread.getCurrentNode();
        ThreadSpec tspec = spec.findThreadSpec(thread.getThreadSpecName());
        Node curNode = tspec.findNode(curNodeName);

        if (isRunningTask(thread, curNode, tspec)) {
            return out;
        }

        // TODO: Replace this with a tuple of Node and Set<Variables>.
        Node nextNode = calculateNextNode(wfRun, thread, curNode, spec);

        return out;
    }

    private static boolean isRunningTask(
        ThreadRunPb.Builder thread, Node curNode, ThreadSpec tspec
    ) {
        int numActiveTasks = thread.getActiveTaskRunsCount();
        if (numActiveTasks == 0) return false;

        TaskRunPb.Builder lastTask = thread.getActiveTaskRunsBuilder(
            numActiveTasks - 1
        );

        return (lastTask.getStatus() == LHStatusPb.RUNNING ||
            lastTask.getStatus() == LHStatusPb.STARTING);
    }

    private static Node calculateNextNode(
        WFRunPb.Builder wfRun, ThreadRunPb.Builder threadRun, Node curNode,
        WFSpec spec
    ) throws VarSubError {
        for (Edge e : curNode.getOutgoingEdges() ) {
            if (evaluateEdge(e, threadRun, wfRun)) {
                return e.getSinkNode();
            }
        }
        throw new RuntimeException("Not possible; means bad WFSpec.");
    }

    private static boolean evaluateEdge(
        Edge e, ThreadRunPb.Builder thread, WFRunPb.Builder wfRun
    ) throws VarSubError {
        if (e.condition == null) return true;

        return evaluateCondition(e.condition, thread, wfRun);
    }

    private static boolean evaluateCondition(
        EdgeCondition c, ThreadRunPb.Builder thread, WFRunPb.Builder wfRun
    ) throws VarSubError {
        VariableValue lhs = assignVariable(c.leftSide, thread, wfRun);
        VariableValue rhs = assignVariable(c.rightSide, thread, wfRun);

        switch (c.comparator) {
        case LESS_THAN:
            return lhs.compare(rhs) < 0;
        case GREATER_THAN:
            return lhs.compare(rhs) > 0;
        case LESS_THAN_EQ:
            return lhs.compare(rhs) <= 0;
        case GREATER_THAN_EQ:
            return lhs.compare(rhs) >= 0;
        case EQUALS:
            return lhs.compare(rhs) == 0;
        case NOT_EQUALS:
            return lhs.compare(rhs) != 0;
        case IN:
            return rhs.contains(lhs);
        case NOT_IN:
            return !rhs.contains(lhs);
        default:
            throw new VarSubError(null, "invalid operand!");
        }
    }

    private static VariableValue assignVariable(
        VariableAssignment assn, ThreadRunPb.Builder thread,
        WFRunPb.Builder wfRun
    ) throws VarSubError {
        if (assn.literalValue != null) {
            return new VariableValue(assn.literalValue);
        }

        VariableValue preJsonpath = null;

        if (assn.wfRunVariableName != null) {
            // TODO: Traverse up the tree of parent/child threads once we enable
            // multithreading.
            VariableValuePbOrBuilder pb = thread.getVariablesOrDefault(
                assn.wfRunVariableName, null
            );
            if (pb == null) {
                throw new VarSubError(
                    null,
                    "Referred to variable " + assn.wfRunVariableName + " which had "
                    + "not been assigned or used yet!"
                );
            }
            preJsonpath = new VariableValue();
            preJsonpath.setSerializedVal(pb.getSerializedValue().toByteArray());
        } else if (assn.defaultValue != null) {
            preJsonpath = new VariableValue(assn.defaultValue);
        } else {
            throw new VarSubError(null, "invalid variable assignment");
        }

        if (assn.jsonPath != null) {
            return preJsonpath.jsonpath(assn.jsonPath);
        }
        return preJsonpath;
    }
}
