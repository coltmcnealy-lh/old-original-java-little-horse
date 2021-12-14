package little.horse.lib;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

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
        System.out.println("\\n\n\n\n\n\n\n");
        System.out.println(event.toString());
        System.out.println(wfRun.toString());

        // Need to figure out if it triggers our trigger. Two possibilities:
        // 1. We're the entrypoint node, so watch out for the WF_RUN_STARTED event.
        // 2. We're not entrypoint, so we watch out for completed tasks of the upstream
        //    nodes.
        if (trigger.triggerEventType != event.type) {
            System.out.println("leaving because wrong trigger event type");
            return;
        }

        if (trigger.triggerEventType == WFEventType.TASK_COMPLETED) {
            TaskRunEndedEventSchema tr;
            try {
                tr = new ObjectMapper().readValue(event.content, TaskRunEndedEventSchema.class);
            } catch (Exception exn) {
                exn.printStackTrace();
                return;
            }
            // Then we gotta make sure we only process the right node's outputs.
            if (!tr.nodeGuid.equals(trigger.triggerNodeGuid)) {
                System.out.println("Guid mismatch: " + tr.nodeGuid + " " + trigger.triggerNodeGuid);
                return;
            }
        }

        System.out.println("about to start the thread");
        Thread thread = new Thread(() -> {
            try {
                this.doAction(wfRun, event.executionNumber + 1);
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
            if (!schema.taskRuns.containsKey(tr.wfNodeName)) {
                schema.taskRuns.put(tr.wfNodeName, new ArrayList<TaskRunSchema>());
            }
            schema.taskRuns.get(tr.wfNodeName).add(tr);
        }

        schema.variables = wfRun.variables;
        return schema;
    }

    public static String getContextString(WFRunSchema wfRun) {
        WFRunVariableContexSchema schema = TaskDaemonEventActor.getContext(wfRun);

        try {
            return new ObjectMapper().writeValueAsString(schema);
        } catch(JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }

    private ArrayList<String> getBashCommand(WFRunSchema wfRun) {
        ArrayList<String> cmd = taskDef.getModel().bashCommand;
        ArrayList<String> newCmd = new ArrayList<String>();
        Pattern p = Constants.VARIABLE_PATTERN;

        System.out.println("Context:\n\n" + getContextString(wfRun));

        for (String arg : cmd) {
            Matcher m = p.matcher(arg);
            if (m.matches()) {
                System.out.println(arg + " matches variable thingy.");
                String varName = arg.substring(2, arg.length() - 2); // hackityhack
                
                // Now we gotta make sure that the varName is actually in the wfRun
                String jsonpath = node.variables.get(varName);
                if (jsonpath == null) {
                    System.out.println("got null");
                    try {
                        System.out.println(new ObjectMapper().writeValueAsString(node));
                        System.out.println(jsonpath);
                        System.out.println(varName);
                        System.out.println(arg);
                        return null;
                    } catch (Exception exn) {
                        exn.printStackTrace();
                    }
                }

                String result = JsonPath.parse(getContextString(wfRun)).read(jsonpath);
                System.out.println("got result: " + result);
                newCmd.add(result);
            } else {
                System.out.println(arg + " doesnt match variable thingy.");
                newCmd.add(arg);
            }
        }

        return newCmd;
    }

    private void doAction(WFRunSchema wfRun, int newExecutionNumber) throws Exception {
        ArrayList<String> command = this.getBashCommand(wfRun);
        ProcessBuilder pb = new ProcessBuilder(command);
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
        tr.nodeGuid = node.guid;
        tr.bashCommand = command;

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
