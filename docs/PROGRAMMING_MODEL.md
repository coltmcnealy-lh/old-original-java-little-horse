# LittleHorse Programming Model

*NOTE: This document describes the constructs available when writing LittleHorse Workflows, and how those constructs behave. This document is a conceptual guide for users; it is neither an API specification nor a design document.*

LittleHorse is a Workflow Engine: a client can define Workflows (thus creating a `WFSpec`), and then submit requests to run a `WFSpec`, thus creating a `WFRun`. LittleHorse provides reliability guarantees out of the box—a `WFRun` execution will either complete or report an error. No work can be done without first being journalled in LittleHorse, and no work will be scheduled and forgotten about.

## What is a Workflow?

A workflow in LittleHorse is a blueprint for running a series of tasks across various workers in a reliable, observable, and scalable manner. A thread (`ThreadSpec`) in a workflow specification (`WFSpec`) essentially consists of a set of `Node`s and a set of `Edge`s between those `Nodes`. A `Node` is a unit of work that often represents the execution of a `TaskRun`. Once a `Node` is executed, any `Nodes` specified by outgoing edges from the just-completed one are scheduled and run next. A `WFRun` is an instance of a `WFSpec`.

The LittleHorse programming model is designed to be as analogous as possible to real programming. As such, you may think of a `ThreadRun` as a running thread in a program. The `ThreadRun` may define local variables, execute tasks (lines of code) conditionally depending on those variables (`if`/`else`), spawn or wait for child threads, be interrupted by an `ExternalEvent`, and fail and throw an exception to its parent.

## Task Execution: `TaskDef` and `TaskQueue`
The core workflow engine manages the scheduling of `TaskRun`'s according to a `WFSpec` (we'll get to that in a minute). How does the scheduling of a `TaskRun` work?

* Every `Node` of type `TASK` has a reference to a `TaskDef`, or Task Definition.
* Every `TaskDef` refers to a `TaskQueue` object, which is uniquely identified by its `name` field. (The `TaskDef` also has an optional `taskType` parameter, which may be used by workflow worker clients.)
* When a `TaskRun` is to be scheduled, LittleHorse pushes a `TaskScheduleRequest` to the appropriate `TaskQueue`. The event contains information about any variables needed to run the task, the `taskType`, correlated `WFRun` and `WFSpec` info, and other potentially useful metadata.
* A Task Worker reads the `TaskScheduleRequest` from the `TaskQueue` and commits the offset (and optionally sends a `TaskStartedEvent` marking the `TaskRun` as started). When the task is completed, the Task Worker notifies LittleHorse via a `TaskRunEndedEvent`.

## Threading Model

