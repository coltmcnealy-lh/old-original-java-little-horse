# Task Worker SDK Overview

In order to execute tasks, users are responsible for deploying some worker of some sort that reads from `TaskQueue`'s, executes tasks, and pushes `TaskRunStartedEvent`'s and `TaskRunEndedEvent`'s back to the `WFRun`'s appropriate kafka topic.

## Using the SDK

The Task Worker Java SDK provides a simple interface to execute tasks.

* First, implement the `TaskExecutor` interface. This is where your task execution logic will live.
* Create an instance of the `TaskWorker`, passing in your `TaskExecutor` and the name of the `TaskQueue` you wish to process.
* Start the worker, and your `TaskExecutor` will begin processing the tasks.

It is important to note that the `TaskWorker` object has a thread pool (the client specifies the size of that thread) and delegates tasks to that threadpool. If you specify a threadpool of size greater than one, it is not safe to store non-atomic local variables in your `TaskExecutor` implementation as they may be modified concurrently by separate threads running different `TaskRuns`.

## Implementing the `TaskExecutor`

You must implement the `executeTask` method, which takes in a `TaskScheduleRequest` and a `WorkerContext` and returns a JSON-serializable object. The object returned by `executeTask` is serialized and stored as the output of the `TaskRunResult`. Any calls to `WorkerContext::log` are printed to stdout on the process and also added into the `stderr` of the `TaskRunResult`.

If `executeTask` throws an exception, the stacktrace is put into the `stderr` of the `TaskRunResult` and the returncode is set to -1.
