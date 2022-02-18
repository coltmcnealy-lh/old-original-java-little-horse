# LittleHorse Programming Model

*NOTE: This document describes the constructs available when writing LittleHorse Workflows, and how those constructs behave. This document is a conceptual guide for users regarding how the system behaves; it is not a formal API specification, nor is it a description of how that API is implemented.*

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

A `TaskRun` may be in any of the following states:
* `SCHEDULED`
* `RUNNING`
* `COMPLETED`
* `FAILED`

## `WFSpec` Primitives
*Note: A `WFSPec` consists of one or more `ThreadSpec`'s and has a single `ThreadSpec` which is designated as the entrypoint. Just like a thread in normal programming, a thread may spawn child threads—those mechanics are discussed below.*

The status of a `WFRun` is simply the status of the entrypoint `ThreadRun`. A `ThreadRun` may be in any of the following states:
* `SCHEDULED`
* `RUNNING`
* `COMPLETED`
* `HALTING`
* `HALTED`
* `FAILING`
* `FAILED`

### `WFRunVariable`
A `ThreadSpec` may define variables to be shared between `Node`'s. Variables are persisted in JSON form, and as such may be of type JSON Object, JSON array, String, Integer, Float, or Boolean.

Variables may be initialized with a default value or as the result of a `Node` which has the `variableMutations` field set (discussed below).

A user of LittleHorse may optionally specify whether they want `ThreadRun`'s and their parent `WFRun`'s to be indexed based on the values of their variables. When enabled, this feature allows a user to query the LittleHorse API to, for example, "give me all `ThreadRun`'s where the variable `customerEmail` is `'gordon.ramsay@gmail.com'`".

### Task Execution
A `Node` may be of type `TASK`, in which case it should specify a `TaskDef` to execute. A `TaskDef` may require input variables, and if so, the `Node` must also specify how to set those input variables using a `VariableAssignment` (discussed below).

When the Node produces output, 

### `VariableAssignment`
`TaskDef`'s and `EdgeCondition`'s often require input variables to either execute a `TaskRun` or deciee whether an `Edge` should be activated or not.  The following methods are supported:
* Assigning the variable a literal value.
* Assigning metadata about the `WFRun`:
  * `WFRun` id
  * `ThreadRun` `id` (a counter) or `guid`
  * `WFSpec` id or name
* Using a `WFRunVariable`:
  * A `wfRunVariableName` is required. It is the name of a variable that must be defined and in scope for the `ThreadRun`.
  * A `jsonpath` may optionally be provided if the variable is a Json Object or Json Array. If a `jsonpath` is provided, the resulting value is the result of evaluating the `jsonpath` on the provided `WFRunVariable`.

If a specified value is of the wrong type, or a `jsonpath` expression fails, the `ThreadRun` is marked as `FAILED` with an appropriate error message.

### Blocking `ExternalEvent`

### Conditional Branching

### Spawning Threads

### Joining Threads

### Interrupt Handlers

### Exception Handlers