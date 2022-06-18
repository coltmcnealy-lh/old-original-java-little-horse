package io.littlehorse.deployers.examples.common.taskimpl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.events.TaskRunEndedEvent;
import io.littlehorse.common.events.TaskRunEvent;
import io.littlehorse.common.events.TaskRunResult;
import io.littlehorse.common.events.TaskRunStartedEvent;
import io.littlehorse.common.events.WFEvent;
import io.littlehorse.common.events.WFEventType;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.metadata.TaskDef;
import io.littlehorse.common.util.LHDatabaseClient;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.deployers.examples.common.DeployerConfig;
import io.littlehorse.scheduler.TaskScheduleRequest;

public class TaskWorker {
    private LHConfig config;
    private TaskDef taskDef;
    private JavaTask executor;
    private ExecutorService threadPool;
    private KafkaConsumer<String, Bytes> consumer;
    private KafkaProducer<String, Bytes> txnProducer;
    private KafkaProducer<String, Bytes> producer;

    public TaskWorker(
        LHConfig config, String taskQueueName, JavaTask executor, int numThreads
    ) throws LHConnectionError {
        this.config = config;
        this.taskDef = LHDatabaseClient.getByNameOrId(
            taskQueueName, config, TaskDef.class
        );
        this.executor = executor;
        this.threadPool = Executors.newFixedThreadPool(numThreads);

        this.txnProducer = config.getTxnProducer();
        this.producer = config.getProducer();
        this.consumer = config.getConsumer(this.taskDef.getKafkaTopic());
    }

    public void run() {
        /*
        The Worker will:
        - Read a TaskScheduleRequest from a TaskQueue's Kafka Topic
        - Determine the Event Log Kafka topic for that WFRun (using the WFSpec)
        - Send a TaskStartedEvent to the Event Log and commit the consumer offset
          for the TaskScheduleRequest that was read
        - Execute the task
        */

        while (true) {
            ConsumerRecords<String, Bytes> records = consumer.poll(
                config.getTaskPollDuration()
            );
            records.forEach(this::consumeAndScheduleTask);
        }
    }

    /**
     * Using the strongest possible consistency guarantees, this method atomically
     * commits the offset of the record AND fires an event to the WFRun topic stating
     * that the associated task was scheduled. Then, the task is queued onto the
     * threadpool.
     * 
     * Since we synchronously wait for the Kafka transaction, there is no way to do
     * work and not know about it. This is the strongest possible guarantee; however,
     * it is also the slowest.
     * @param record the record containing a TaskScheduleRequest.
     */
    private void consumeAndScheduleTask(ConsumerRecord<String, Bytes> record) {

        TaskScheduleRequest schedReq;
        try {
            schedReq = BaseSchema.fromBytes(
                record.value().get(),
                TaskScheduleRequest.class,
                config
            );
            LHUtil.log("Processing:", schedReq);
        } catch(LHSerdeError exn) {
            exn.printStackTrace();
            return;
        }

        this.txnProducer.beginTransaction();
        // First, send offsets to transaction.
        HashMap<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<
            TopicPartition, OffsetAndMetadata
        >();

        TopicPartition tp = new TopicPartition(
            record.topic(), record.partition()
        );
        OffsetAndMetadata om = new OffsetAndMetadata(
            record.offset(), record.leaderEpoch(), "Committed by worker."
        );
        offsetsToCommit.put(tp, om);
        this.txnProducer.sendOffsetsToTransaction(
            offsetsToCommit, this.consumer.groupMetadata()
        );

        TaskRunStartedEvent trs = new TaskRunStartedEvent();
        trs.threadId = schedReq.threadId;
        trs.workerId = "some-worker-id";

        TaskRunEvent tre = new TaskRunEvent();
        tre.taskDefVersionNumber = taskDef.versionNumber;
        tre.startedEvent = trs;
        tre.taskRunPosition = schedReq.taskRunPosition;
        tre.timestamp = LHUtil.now();

        WFEvent event = new WFEvent();
        event.content = tre.toString();
        event.wfRunId = schedReq.wfRunId;
        event.wfSpecId = schedReq.wfSpecId;
        event.wfSpecName = schedReq.wfSpecName;
        event.type = WFEventType.TASK_EVENT;
        event.timestamp = tre.timestamp;
        event.threadId = schedReq.threadId;

        ProducerRecord<String, Bytes> prodRecord = new ProducerRecord<String, Bytes>(
            schedReq.kafkaTopic, schedReq.wfRunId, new Bytes(event.toBytes())
        );
        this.txnProducer.send(prodRecord);
        this.txnProducer.commitTransaction();
        // Now, execute the actual thing.
        this.threadPool.submit(() -> { this.herdCats(schedReq);});            
    }

    private void herdCats(TaskScheduleRequest schedReq) {
        WorkerContext ctx = new WorkerContext(config, schedReq);

        Object output = null;
        TaskRunResult result = new TaskRunResult();
        try {
            output = this.executor.executeTask(schedReq, ctx);
            try {
                result.stdout = LHUtil.getObjectMapper(
                    config
                ).writeValueAsString(output);
                result.stderr = ctx.getStderr();
            } catch (Exception exn) {
                exn.printStackTrace();
                result.stdout = output == null ? null : output.toString();
            }
            result.success = true;
        } catch (Exception exn) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exn.printStackTrace(pw);
            result.stderr = sw.toString();
            if (ctx.getStderr() != null) {
                result.stderr += "\n\n\n\n" + ctx.getStderr();
            }
            result.returncode = -1;
            result.success = false;
        }

        TaskRunEndedEvent out = new TaskRunEndedEvent();
        out.result = result;
        out.taskRunPosition = schedReq.taskRunPosition;
        out.threadId = schedReq.threadId;

        TaskRunEvent tre2 = new TaskRunEvent();
        tre2.taskDefVersionNumber = taskDef.versionNumber;
        tre2.endedEvent = out;
        tre2.taskRunPosition = schedReq.taskRunPosition;
        tre2.timestamp = LHUtil.now();

        WFEvent completedEvent = new WFEvent();
        completedEvent.wfRunId = schedReq.wfRunId;
        completedEvent.threadId = schedReq.threadId;
        completedEvent.timestamp = tre2.timestamp;
        completedEvent.wfSpecId = schedReq.wfSpecId;
        completedEvent.wfSpecName = schedReq.wfSpecName;
        completedEvent.content = tre2.toString();
        completedEvent.type = WFEventType.TASK_EVENT;

        LHUtil.log(completedEvent.toString());

        ProducerRecord<String, Bytes> completedRecord = new ProducerRecord<
            String, Bytes
        >(
            schedReq.kafkaTopic, schedReq.wfRunId, new Bytes(
                completedEvent.toBytes()
            )
        );

        this.producer.send(completedRecord);
    }

    public static void main(String[] args) throws LHConnectionError {
        LHUtil.log("Running Task Worker executable!");

        LHConfig config = new LHConfig();
        DeployerConfig ddConfig = new DeployerConfig();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            config.cleanup();
        }));
        JavaTask executor = ddConfig.getTaskExecutor();
        executor.init(ddConfig, config);
        TaskDef taskDef = ddConfig.lookupTaskDefOrDie(config);

        new TaskWorker(
            config, taskDef.name, executor, ddConfig.getNumThreads()
        ).run();
    }
}
