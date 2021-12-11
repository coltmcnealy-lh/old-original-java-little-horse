package little.horse.lib;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

public class TaskDaemonEventActor implements WFEventProcessorActor {
    private WFSpec wfSpec;
    private NodeSchema node;
    private Config config;
    private WFTriggerSchema trigger;
    private TaskDef taskDef;

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
        System.out.println(event.toString());
        System.out.println(wfRun.toString());

        // Need to figure out if it triggers our trigger. Two possibilities:
        // 1. We're the entrypoint node, so watch out for the WF_RUN_STARTED event.
        // 2. We're not entrypoint, so we watch out for completed tasks of the upstream
        //    nodes.
        if (trigger.triggerEventType != event.type) {
            return;
        }
        
        if (trigger.triggerEventType == WFEventType.TASK_COMPLETED) {
            // Then we gotta make sure we only process the right node's outputs.
            TaskRunSchema lastTR = wfRun.taskRuns.get(wfRun.taskRuns.size() - 1);
            if (lastTR.wfNodeGuid != trigger.triggerNodeGuid) {
                return;
            }
        }

        Thread thread = new Thread(() -> {
            try {
                this.doAction(wfRun, event.executionNumber + 1);
            } catch(Exception exn) {
                exn.printStackTrace();
            }
        });

        thread.start();
    }

    private void doAction(WFRunSchema wfRun, int newExecutionNumber) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
            taskDef.getModel().bashCommand
        );
        Process proc;
        proc = pb.start();
        BufferedReader stdoutReader = new BufferedReader(
            new InputStreamReader(proc.getInputStream())
        );
        BufferedReader stderrReader = new BufferedReader(
            new InputStreamReader(proc.getErrorStream())
        );

        if (taskDef.getModel().stdin != null) {
            proc.getOutputStream().write(taskDef.getModel().stdin.getBytes());
        }
        proc.getOutputStream().close();
        proc.waitFor();

        StringBuilder stdoutBuilder = new StringBuilder();
        StringBuilder stderrBuilder = new StringBuilder();

        String line = stdoutReader.readLine();
        while (line != null) {
            stdoutBuilder.append(line);
            line = stdoutReader.readLine();
        }
        line = stderrReader.readLine();
        while (line != null) {
            stderrBuilder.append(line);
            line = stderrReader.readLine();
        }

        TaskRunEndedEventSchema tr = new TaskRunEndedEventSchema();
        tr.stdout = stdoutBuilder.toString();
        tr.stderr = stderrBuilder.toString();
        tr.returncode = proc.exitValue();

        WFEventSchema event = new WFEventSchema();
        event.content = new ObjectMapper().writeValueAsString(tr);
        event.timestamp = new Date();
        event.executionNumber = newExecutionNumber + 1;
        event.type = WFEventType.TASK_COMPLETED;
        event.wfRunGuid = wfRun.guid;
        event.wfSpecGuid = wfRun.wfSpecGuid;
        event.wfSpecName = wfRun.wfSpecName;

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            wfSpec.getModel().kafkaTopic,
            wfRun.guid,
            new ObjectMapper().writeValueAsString(event)
        );
        config.send(record);
    }
}
