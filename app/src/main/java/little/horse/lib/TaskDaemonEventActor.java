package little.horse.lib;

import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.objects.TaskDef;
import little.horse.lib.objects.WFRun;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.NodeCompletedEventSchema;
import little.horse.lib.schemas.TaskRunFailedEventSchema;
import little.horse.lib.schemas.TaskRunStartedEventSchema;
import little.horse.lib.schemas.VariableAssignmentSchema;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFTriggerSchema;

public class TaskDaemonEventActor implements WFEventProcessorActor {
    private WFSpec wfSpec;
    private NodeSchema node;
    private Config config;
    private WFTriggerSchema trigger;
    private TaskDef taskDef;

    public String getNodeGuid() {
        return node.guid;
    }

    public TaskDaemonEventActor(
        WFSpec wfSpec,
        NodeSchema node,
        TaskDef taskDef,
        Config config
    ) {
        this.wfSpec = wfSpec;
        this.node = node;
        this.config = config;
        this.taskDef = taskDef;

        // TODO: Figure out what to do for multiple incoming sources.
        this.trigger = this.node.triggers.get(0);
    }

    public void act(WFRunSchema wfRun, int taskRunNumber) {
        // Need to figure out if it triggers our trigger. Two possibilities:
        // 1. We're the entrypoint node, so watch out for the WF_RUN_STARTED event.
        // 2. We're not entrypoint, so we watch out for completed tasks of the upstream
        //    nodes.

        // if (trigger.triggerEventType != event.type) {
        //     return;
        // }

        // NodeCompletedEventSchema tr = null;
        // if (trigger.triggerEventType == WFEventType.NODE_COMPLETED) {
        //     tr = BaseSchema.fromString(
        //         event.content, NodeCompletedEventSchema.class
        //     );
        //     if (tr == null) {
        //         return;
        //     }
        //     // Then we gotta make sure we only process the right node's outputs.
        //     if (!tr.nodeGuid.equals(trigger.triggerNodeGuid)) {
        //         return;
        //     }
        // }

        Thread thread = new Thread(() -> {
            try {
                this.doAction(wfRun, taskRunNumber);
            } catch(Exception exn) {
                exn.printStackTrace();
            }
        });

        thread.start();
    }

    private ArrayList<String> getBashCommand(WFRunSchema wfRun)
        throws VarSubOrzDash
    {
        ArrayList<String> cmd = taskDef.getModel().bashCommand;
        ArrayList<String> newCmd = new ArrayList<String>();
        Pattern p = Constants.VARIABLE_PATTERN;

        for (String arg : cmd) {
            Matcher m = p.matcher(arg);
            if (m.matches()) {
                String varName = arg.substring(2, arg.length() - 2); // hackityhack
                
                // Now we gotta make sure that the varName is actually in the wfRun
                VariableAssignmentSchema var = node.variables.get(varName);

                if (var == null) {
                    throw new VarSubOrzDash(
                        null,
                        "WFSpec doesnt assign var " + varName+ " on node " + node.name
                    );
                }

                String substitutionResult = WFRun.getVariableSubstitution(
                    wfRun, var
                ).toString();
                newCmd.add(substitutionResult);
            } else {
                newCmd.add(arg);
            }
        }

        return newCmd;
    }

    private void doAction(WFRunSchema wfRun, int taskRunNumber) throws Exception {
        ArrayList<String> command;
        try {
            command = this.getBashCommand(wfRun);
        } catch(VarSubOrzDash exn) {
            exn.exn.printStackTrace();
            String message = "Failed looking up a variable in the workflow context\n";
            message += exn.message;

            TaskRunFailedEventSchema trf = new TaskRunFailedEventSchema();
            trf.message = message;
            trf.reason = LHFailureReason.VARIABLE_LOOKUP_ERROR;
            trf.taskRunNumber = taskRunNumber;
            trf.nodeGuid = node.guid;

            WFEventSchema event = new WFEventSchema();
            event.type = WFEventType.TASK_FAILED;
            event.content = trf.toString();
            event.timestamp = LHUtil.now();
            event.wfRunGuid = wfRun.guid;
            event.wfSpecGuid = wfSpec.getModel().guid;
            event.wfSpecName = wfSpec.getModel().name;

            config.send(new ProducerRecord<String, String>(
                wfSpec.getModel().kafkaTopic,
                wfRun.guid,
                event.toString()
            ));
            return;
        }

        // Mark the task as started.
        TaskRunStartedEventSchema trs = new TaskRunStartedEventSchema();
        trs.stdin = null;
        trs.nodeName = node.name;
        trs.nodeGuid = node.guid;
        trs.taskRunNumber = taskRunNumber;
        trs.bashCommand = command;

        WFEventSchema taskStartedEvent = new WFEventSchema();
        taskStartedEvent.content = trs.toString();
        taskStartedEvent.timestamp = new Date();
        taskStartedEvent.type = WFEventType.TASK_STARTED;
        taskStartedEvent.wfRunGuid = wfRun.guid;
        taskStartedEvent.wfSpecGuid = wfRun.wfSpecGuid;
        taskStartedEvent.wfSpecName = wfRun.wfSpecName;

        ProducerRecord<String, String> taskStartRecord = new ProducerRecord<String, String>(
            wfSpec.getModel().kafkaTopic,
            wfRun.guid,
            taskStartedEvent.toString()
        );
        config.send(taskStartRecord);

        ProcessBuilder pb = new ProcessBuilder(command);
        Process proc;
        proc = pb.start();

        if (taskDef.getModel().stdin != null) {
            proc.getOutputStream().write(taskDef.getModel().stdin.getBytes());
        }
        proc.getOutputStream().close();
        proc.waitFor();

        NodeCompletedEventSchema tr;
        boolean success = (proc.exitValue() == 0);
        tr = success ? new NodeCompletedEventSchema() : new TaskRunFailedEventSchema();

        tr.stdout = LHUtil.inputStreamToString(proc.getInputStream());
        tr.stderr = LHUtil.inputStreamToString(proc.getErrorStream());
        tr.returncode = proc.exitValue();
        tr.nodeGuid = node.guid;
        tr.bashCommand = command;
        tr.taskRunNumber = taskRunNumber;

        if (!success) {
            TaskRunFailedEventSchema trf = (TaskRunFailedEventSchema) tr;
            trf.reason = LHFailureReason.TASK_FAILURE;
            trf.message = "got a nonzero exit code!";
        }

        WFEventSchema event = new WFEventSchema();
        event.content = tr.toString();
        event.timestamp = new Date();
        event.type = success ? WFEventType.NODE_COMPLETED : WFEventType.TASK_FAILED;
        event.wfRunGuid = wfRun.guid;
        event.wfSpecGuid = wfRun.wfSpecGuid;
        event.wfSpecName = wfRun.wfSpecName;

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfSpec.getModel().kafkaTopic,
            wfRun.guid,
            event.toString()
        );
        config.send(record);
    }
}
