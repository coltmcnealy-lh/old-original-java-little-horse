package little.horse.lib;

import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class WFEventProcessor
    implements Processor<String, WFEventSchema, String, WFRunSchema>
{
    private KeyValueStore<String, WFRunSchema> kvStore;
    private ProcessorContext<String, WFRunSchema> context;
    private WFEventProcessorActor actor;
    private Config config;

    public WFEventProcessor(WFEventProcessorActor actor, Config config) {
        this.actor = actor;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<String, WFRunSchema> context) {
        kvStore = context.getStateStore(Constants.WF_RUN_STORE);
        this.context = context;
    }

    @Override
    public void process(final Record<String, WFEventSchema> record) {
        // First, we gotta update the WFRun.
        String wfRunGuid = record.key();
        WFEventSchema event = record.value();

        WFRunSchema wfRun = kvStore.get(wfRunGuid);
        if (wfRun == null) {
            if (event.type != WFEventType.WF_RUN_STARTED) {
                raiseError();
                return;
            }
            WFRunRequestSchema runRequest;
            try {
                runRequest = new ObjectMapper().readValue(
                    event.content, WFRunRequestSchema.class
                );
            } catch (Exception exn) {
                exn.printStackTrace();
                raiseError();
                return;
            }

            wfRun = new WFRunSchema();
            wfRun.guid = wfRunGuid;
            wfRun.wfSpecGuid = event.wfSpecGuid;
            wfRun.wfSpecName = event.wfSpecName;
            wfRun.inputVariables = runRequest.inputVariables;
            wfRun.variables = runRequest.variables;
            wfRun.status = LHStatus.RUNNING;
            wfRun.taskRuns = new ArrayList<TaskRunSchema>();

            // Figure out the input, and schedule the TaskRun.
            WFSpec wfSpec = this.getWFSpec(event.wfSpecGuid);
            WFSpecSchema wfSpecSchema = wfSpec.getModel();
            NodeSchema node = wfSpecSchema.nodes.get(
                wfSpecSchema.entrypointNodeName
            );

            TaskRunSchema tr = new TaskRunSchema();
            tr.wfSpecGuid = event.wfSpecGuid;
            tr.wfRunGuid = event.wfRunGuid;
            tr.wfSpecName = event.wfSpecName;
            tr.wfNodeName = node.name;
            tr.wfNodeGuid = node.guid;
            tr.executionNumber = 0;
            tr.status = LHStatus.PENDING;
            wfRun.taskRuns.add(tr);

            kvStore.put(wfRunGuid, wfRun);
        } else {
            if (event.type == WFEventType.WF_RUN_STARTED) {
                raiseError();
                return;
            } else if (event.type == WFEventType.TASK_STARTED) {
                // The one in question should be the last TaskRun in the list.
                // Thus it should be in the PENDING state. If not, raise error.
                TaskRunSchema runningTask = wfRun.taskRuns.get(wfRun.taskRuns.size() -1);
                if (runningTask.status != LHStatus.PENDING) {
                    raiseError();
                    return;
                }
                runningTask.status = LHStatus.RUNNING;
                runningTask.startTime = event.timestamp;

                TaskRunStartedEventSchema taskRunStart;
                try {
                    taskRunStart = new ObjectMapper().readValue(
                        event.toString(), TaskRunStartedEventSchema.class
                    );
                } catch (Exception exn) {
                    exn.printStackTrace();
                    raiseError();
                    return;
                }
                runningTask.stdin = taskRunStart.stdin;
                runningTask.bashCommand = taskRunStart.bashCommand;
                runningTask.dockerImage = taskRunStart.dockerImage;

                kvStore.put(wfRunGuid, wfRun);

            } else if (event.type == WFEventType.TASK_COMPLETED) {
                TaskRunSchema runningTask = wfRun.taskRuns.get(wfRun.taskRuns.size() -1);
                if (runningTask.status != LHStatus.PENDING) {
                    raiseError();
                    return;
                }
                runningTask.status = LHStatus.COMPLETED;
                runningTask.endTime = event.timestamp;

                TaskRunEndedEventSchema taskRunEnd;
                try {
                    taskRunEnd = new ObjectMapper().readValue(
                        event.toString(),
                        TaskRunEndedEventSchema.class
                    );
                } catch (Exception exn) {
                    exn.printStackTrace();
                    raiseError();
                    return;
                }

                runningTask.stderr = taskRunEnd.stderr;
                runningTask.stdout = taskRunEnd.stdout;
                runningTask.returnCode = taskRunEnd.returncode;

                // Now see if any task runs need to be scheduled.
                int numTaskRunsScheduled = 0;
                WFSpec wfSpec = getWFSpec(wfRun.wfSpecGuid);
                NodeSchema curNode = wfSpec.getModel().nodes.get(runningTask.wfNodeName);
                for (EdgeSchema edge : curNode.outgoingEdges) {
                    // TODO: for conditional branching, decide whether edge is active.
                    numTaskRunsScheduled++;
                    TaskRunSchema tr = new TaskRunSchema();
                    NodeSchema destNode = wfSpec.getModel().nodes.get(edge.sinkNodeName);
                    tr.wfNodeGuid = destNode.guid;
                    tr.wfNodeName = destNode.name;
                    tr.status = LHStatus.PENDING;
                    tr.wfSpecGuid = event.wfSpecGuid;
                    tr.wfRunGuid = event.wfRunGuid;
                    tr.wfSpecName = event.wfSpecName;

                    // TODO: Handle multithreading.
                    tr.executionNumber = event.executionNumber + 1;
                    wfRun.taskRuns.add(tr);
                }
                if (numTaskRunsScheduled == 0) {
                    wfRun.status = LHStatus.COMPLETED;
                } else if (numTaskRunsScheduled == 1) {
                    // Nothing to do.
                } else {
                    System.out.println("orzdash we can't do multiple things");
                    raiseError();
                    return;
                }

                kvStore.put(wfRunGuid, wfRun);
            }
        }

        actor.act(wfRun, event);
        context.forward(new Record<String, WFRunSchema>(
            wfRunGuid, wfRun, event.timestamp.getTime()
        ));
    }

    private WFSpec getWFSpec(String guid) {
        // TODO: Do some caching here
        try {
            return WFSpec.fromIdentifier(guid, config);
        } catch (Exception exn) {
            raiseError();
            return null;
        }
    }
    
    private void raiseError() {
        System.out.println("Raising error!!");
    }
    
}
