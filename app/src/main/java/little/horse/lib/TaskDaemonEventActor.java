package little.horse.lib;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.TaskDefSchema;
import little.horse.lib.schemas.TaskRunEndedEventSchema;
import little.horse.lib.schemas.TaskRunEventSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.TaskRunStartedEventSchema;
import little.horse.lib.schemas.VariableAssignmentSchema;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFSpecSchema;
import little.horse.lib.schemas.ThreadRunSchema;

public class TaskDaemonEventActor implements WFEventProcessorActor {
    private NodeSchema node;
    private Config config;
    private TaskDefSchema taskDef;
    // private ThreadSpecSchema threadSpec;

    public String getNodeGuid() {
        return node.guid;
    }

    public TaskDaemonEventActor(
        WFSpecSchema wfSpec,
        NodeSchema node,
        TaskDefSchema taskDef,
        Config config
    ) {
        this.config = config;
        this.node = node;
        this.taskDef = taskDef;

        this.node.setConfig(this.config);
        this.taskDef.setConfig(this.config);
    }

    public void act(WFRunSchema wfRun, int threadNumber, int taskRunNumber) {

        Thread thread = new Thread(() -> {
            try {
                this.doAction(wfRun, threadNumber, taskRunNumber);
            } catch(Exception exn) {
                exn.printStackTrace();
            }
        });

        thread.start();
    }

    private ArrayList<String> getBashCommand(WFRunSchema wfRun, ThreadRunSchema thread)
        throws VarSubOrzDash, LHLookupException, LHNoConfigException
    {
        ArrayList<String> cmd = taskDef.bashCommand;
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
                Object varResult = thread.assignVariable(
                    var
                );
                newCmd.add(varResult.toString());
            } else {
                newCmd.add(arg);
            }
        }

        return newCmd;
    }

    private TaskRunEventSchema getTaskRunEventSchema(ThreadRunSchema thread) {
        TaskRunSchema tr = thread.taskRuns.get(thread.taskRuns.size() - 1);
        TaskRunEventSchema out = new TaskRunEventSchema();
        out.threadID = tr.threadID;
        out.taskRunNumber = tr.number;
        out.timestamp = LHUtil.now(); // can be set again later.
        return out;
    }

    private void finishTask(
        ThreadRunSchema thread, String message, int returnCode,
        String stdout, String stderr, LHFailureReason reason
    ) throws LHLookupException, LHNoConfigException {
        TaskRunEventSchema event = getTaskRunEventSchema(thread);

        TaskRunEndedEventSchema ee = new TaskRunEndedEventSchema();
        ee.message = message;
        ee.returncode = returnCode;
        ee.stdout = stdout;
        ee.stderr = stderr;
        ee.reason = reason;
        ee.success = returnCode == 0;

        event.endedEvent = ee;

        thread.wfRun.newWFEvent(WFEventType.TASK_EVENT, event).record();
    }

    private void doAction(WFRunSchema wfRun, int threadNumber, int taskRunNumber)
    throws Exception {
        ArrayList<String> command;
        ThreadRunSchema thread = wfRun.threadRuns.get(threadNumber);
        try {
            command = this.getBashCommand(wfRun, thread);
        } catch(VarSubOrzDash exn) {
            exn.exn.printStackTrace();
            String message = "Failed looking up a variable in the workflow context\n";
            message += exn.message;

            finishTask(
                thread, message, -1, null, null,
                LHFailureReason.VARIABLE_LOOKUP_ERROR
            );
            return;
        }

        // Mark the task as started.
        TaskRunStartedEventSchema trs = new TaskRunStartedEventSchema();
        trs.stdin = null;
        trs.nodeName = node.name;
        trs.nodeGuid = node.guid;
        trs.taskRunNumber = taskRunNumber;
        trs.threadID = threadNumber;
        trs.bashCommand = command;

        WFEventSchema taskStartedEvent = wfRun.newWFEvent(
            WFEventType.TASK_EVENT, trs
        );
        taskStartedEvent.record();

        ProcessBuilder pb = new ProcessBuilder(command);
        Process proc;
        proc = pb.start();

        if (taskDef.stdin != null) {
            proc.getOutputStream().write(taskDef.stdin.getBytes());
        }
        proc.getOutputStream().close();
        proc.waitFor();

        finishTask(
            thread, null, proc.exitValue(),
            LHUtil.inputStreamToString(proc.getInputStream()),
            LHUtil.inputStreamToString(proc.getErrorStream()),
            null
        );
    }
}
