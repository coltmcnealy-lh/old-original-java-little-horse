package little.horse.lib.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.TaskQueue;
import little.horse.common.util.LHDatabaseClient;

public class TaskWorker {
    private Config config;
    private TaskQueue taskQueue;
    private TaskExecutor executor;
    private ExecutorService threadPool;

    public TaskWorker(
        Config config, String taskQueueName, TaskExecutor executor, int numThreads
    ) throws LHConnectionError {
        this.config = config;
        this.taskQueue = LHDatabaseClient.lookupMeta(
            taskQueueName, config, TaskQueue.class
        );
        this.executor = executor;
        this.threadPool = Executors.newFixedThreadPool(numThreads);
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
            
        }
    }
}
