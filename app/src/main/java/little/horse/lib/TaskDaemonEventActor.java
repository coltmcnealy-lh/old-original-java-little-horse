package little.horse.lib;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import org.apache.kafka.clients.producer.ProducerRecord;

import little.horse.lib.objects.TaskDef;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.TaskRunEndedEventSchema;
import little.horse.lib.schemas.TaskRunFailedEventSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.TaskRunStartedEventSchema;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFRunVariableContexSchema;
import little.horse.lib.schemas.WFTriggerSchema;

public class TaskDaemonEventActor implements WFEventProcessorActor {
    private WFSpec wfSpec;
    private NodeSchema node;
    private Config config;
    private WFTriggerSchema trigger;
    private TaskDef taskDef;

    private class VarSubOrzDash extends Exception {
        public String varName;
        public String jsonpath;
        public PathNotFoundException exn;

        public VarSubOrzDash(PathNotFoundException exn, String varName, String jsonpath) {
            this.exn = exn;
            this.varName = varName;
            this.jsonpath = jsonpath;
        }
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

    public void act(WFRunSchema wfRun, WFEventSchema event) {
        // Need to figure out if it triggers our trigger. Two possibilities:
        // 1. We're the entrypoint node, so watch out for the WF_RUN_STARTED event.
        // 2. We're not entrypoint, so we watch out for completed tasks of the upstream
        //    nodes.
        if (trigger.triggerEventType != event.type) {
            return;
        }

        TaskRunEndedEventSchema tr = null;
        if (trigger.triggerEventType == WFEventType.TASK_COMPLETED) {
            tr = BaseSchema.fromString(
                event.content, TaskRunEndedEventSchema.class
            );
            if (tr == null) {
                return;
            }
            // Then we gotta make sure we only process the right node's outputs.
            if (!tr.nodeGuid.equals(trigger.triggerNodeGuid)) {
                return;
            }
        }
        int newExecutionNumber = (tr == null) ? 0 : tr.taskExecutionNumber + 1;

        Thread thread = new Thread(() -> {
            try {
                this.doAction(wfRun, newExecutionNumber);
            } catch(Exception exn) {
                exn.printStackTrace();
            }
        });

        thread.start();
    }

    // TODO: Maybe move this into the wfRunSchema class...?
    public static WFRunVariableContexSchema getContext(WFRunSchema wfRun) {
        WFRunVariableContexSchema schema = new WFRunVariableContexSchema();

        schema.inputVariables = wfRun.inputVariables;
        schema.taskRuns = new HashMap<String, ArrayList<TaskRunSchema>>();
        for (TaskRunSchema tr : wfRun.taskRuns) {
            if (!schema.taskRuns.containsKey(tr.nodeName)) {
                schema.taskRuns.put(tr.nodeName, new ArrayList<TaskRunSchema>());
            }
            schema.taskRuns.get(tr.nodeName).add(tr);
        }

        schema.variables = wfRun.variables;
        return schema;
    }

    public static String getContextString(WFRunSchema wfRun) {
        WFRunVariableContexSchema schema = TaskDaemonEventActor.getContext(wfRun);
        return schema.toString();
    }

    private ArrayList<String> getBashCommand(WFRunSchema wfRun) throws VarSubOrzDash {
        ArrayList<String> cmd = taskDef.getModel().bashCommand;
        ArrayList<String> newCmd = new ArrayList<String>();
        Pattern p = Constants.VARIABLE_PATTERN;

        for (String arg : cmd) {
            Matcher m = p.matcher(arg);
            if (m.matches()) {
                String varName = arg.substring(2, arg.length() - 2); // hackityhack
                
                // Now we gotta make sure that the varName is actually in the wfRun
                String jsonpath = node.variables.get(varName);
                if (jsonpath == null) {
                    return null;
                }
                try {
                    String result = JsonPath.parse(getContextString(wfRun)).read(jsonpath);
                    newCmd.add(result);
                } catch (PathNotFoundException exn) {
                    throw new VarSubOrzDash(
                        exn, varName, jsonpath
                    );
                }
            } else {
                newCmd.add(arg);
            }
        }

        return newCmd;
    }

    private void doAction(WFRunSchema wfRun, int executionNumber) throws Exception {
        ArrayList<String> command;
        try {
            command = this.getBashCommand(wfRun);
        } catch(VarSubOrzDash exn) {
            exn.exn.printStackTrace();
            String message = "Failed looking up a variable in the workflow context";
            message += "\nVarName: " + exn.varName;
            message += "\nJsonPath: " + exn.jsonpath;
            message += "\nContext " + getContextString(wfRun);

            TaskRunFailedEventSchema trf = new TaskRunFailedEventSchema();
            trf.message = message;
            trf.reason = LHFailureReason.VARIABLE_LOOKUP_ERROR;
            trf.taskExecutionNumber = executionNumber;
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
        trs.taskExecutionNumber = executionNumber;
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

        TaskRunEndedEventSchema tr;
        boolean success = (proc.exitValue() == 0);
        tr = success ? new TaskRunEndedEventSchema() : new TaskRunFailedEventSchema();

        tr.stdout = LHUtil.inputStreamToString(proc.getInputStream());
        tr.stderr = LHUtil.inputStreamToString(proc.getErrorStream());
        tr.returncode = proc.exitValue();
        tr.nodeGuid = node.guid;
        tr.bashCommand = command;
        tr.taskExecutionNumber = executionNumber;

        if (!success) {
            TaskRunFailedEventSchema trf = (TaskRunFailedEventSchema) tr;
            trf.reason = LHFailureReason.TASK_FAILURE;
            trf.message = "got a nonzero exit code!";
        }

        WFEventSchema event = new WFEventSchema();
        event.content = tr.toString();
        event.timestamp = new Date();
        event.type = success ? WFEventType.TASK_COMPLETED : WFEventType.TASK_FAILED;
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
